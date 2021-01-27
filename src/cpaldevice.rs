use audiodevice::*;
use config;
use config::{ConfigError, SampleFormat};
use conversions::{
    chunk_to_queue_float, chunk_to_queue_int, queue_to_chunk_float, queue_to_chunk_int,
};
use countertimer;
use cpal;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::Device;
use cpal::{BufferSize, ChannelCount, HostId, SampleRate, StreamConfig};
use rubato::Resampler;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier, RwLock};
use std::thread;

use crate::{CaptureStatus, PlaybackStatus};
use CommandMessage;
use NewValue;
use PrcFmt;
use ProcessingState;
use Res;
use StatusMessage;

pub struct GlobalHost {
    host: Option<cpal::Host>
}

lazy_static! {
    pub static ref CPALHOST: Arc<GlobalHost> = Arc::new(GlobalHost {
        host: None,
    });
}


#[derive(Clone, Debug)]
pub enum CpalHost {
    #[cfg(target_os = "macos")]
    CoreAudio,
    #[cfg(target_os = "windows")]
    Wasapi,
}

#[derive(Clone, Debug)]
pub struct CpalPlaybackDevice {
    pub devname: String,
    pub host: CpalHost,
    pub samplerate: usize,
    pub chunksize: usize,
    pub channels: usize,
    pub sample_format: SampleFormat,
    pub target_level: usize,
    pub adjust_period: f32,
    pub enable_rate_adjust: bool,
}

#[derive(Clone, Debug)]
pub struct CpalCaptureDevice {
    pub devname: String,
    pub host: CpalHost,
    pub samplerate: usize,
    pub resampler_conf: config::Resampler,
    pub enable_resampling: bool,
    pub capture_samplerate: usize,
    pub chunksize: usize,
    pub channels: usize,
    pub sample_format: SampleFormat,
    pub silence_threshold: PrcFmt,
    pub silence_timeout: PrcFmt,
}

fn open_cpal_playback(
    host_cfg: CpalHost,
    devname: &str,
    samplerate: usize,
    channels: usize,
    sample_format: &SampleFormat,
) -> Res<(Device, StreamConfig, cpal::SampleFormat)> {
    let host_id = match host_cfg {
        #[cfg(target_os = "macos")]
        CpalHost::CoreAudio => HostId::CoreAudio,
        #[cfg(target_os = "windows")]
        CpalHost::Wasapi => HostId::Wasapi,
    };
    let host = if let Some(existing_host) = CPALHOST.host {
        existing_host
    }
    else {
        let new_host = cpal::host_from_id(host_id)?;
        CPALHOST.host = Some(new_host);
        new_host
    };
    let mut devices = host.devices()?;
    let device = match devices.find(|dev| match dev.name() {
        Ok(n) => n == devname,
        _ => false,
    }) {
        Some(dev) => dev,
        None => {
            let msg = format!("Could not find device '{}'", devname);
            return Err(ConfigError::new(&msg).into());
        }
    };
    let cpal_format = match sample_format {
        SampleFormat::S16LE => cpal::SampleFormat::I16,
        SampleFormat::FLOAT32LE => cpal::SampleFormat::F32,
        _ => panic!("Unsupported sample format"),
    };
    let stream_config = StreamConfig {
        channels: channels as ChannelCount,
        sample_rate: SampleRate(samplerate as u32),
        buffer_size: BufferSize::Default,
    };
    debug!("Opened CPAL playback device {}", devname);
    Ok((device, stream_config, cpal_format))
}

fn open_cpal_capture(
    host_cfg: CpalHost,
    devname: &str,
    samplerate: usize,
    channels: usize,
    sample_format: &SampleFormat,
) -> Res<(Device, StreamConfig, cpal::SampleFormat)> {
    let host_id = match host_cfg {
        #[cfg(target_os = "macos")]
        CpalHost::CoreAudio => HostId::CoreAudio,
        #[cfg(target_os = "windows")]
        CpalHost::Wasapi => HostId::Wasapi,
    };
    let host = cpal::host_from_id(host_id)?;
    let mut devices = host.devices()?;
    let device = match devices.find(|dev| match dev.name() {
        Ok(n) => n == devname,
        _ => false,
    }) {
        Some(dev) => dev,
        None => {
            let msg = format!("Could not find device '{}'", devname);
            return Err(ConfigError::new(&msg).into());
        }
    };
    let cpal_format = match sample_format {
        SampleFormat::S16LE => cpal::SampleFormat::I16,
        SampleFormat::FLOAT32LE => cpal::SampleFormat::F32,
        _ => panic!("Unsupported sample format"),
    };
    let stream_config = StreamConfig {
        channels: channels as ChannelCount,
        sample_rate: SampleRate(samplerate as u32),
        buffer_size: BufferSize::Default,
    };
    debug!("Opened CPAL capture device {}", devname);
    Ok((device, stream_config, cpal_format))
}

fn write_data_to_device<T>(output: &mut [T], queue: &mut VecDeque<T>)
where
    T: cpal::Sample,
{
    for sample in output.iter_mut() {
        *sample = queue.pop_front().unwrap();
    }
}

/// Start a playback thread listening for AudioMessages via a channel.
impl PlaybackDevice for CpalPlaybackDevice {
    fn start(
        &mut self,
        channel: mpsc::Receiver<AudioMessage>,
        barrier: Arc<Barrier>,
        status_channel: mpsc::Sender<StatusMessage>,
        playback_status: Arc<RwLock<PlaybackStatus>>,
    ) -> Res<Box<thread::JoinHandle<()>>> {
        let devname = self.devname.clone();
        let host_cfg = self.host.clone();
        let samplerate = self.samplerate;
        let chunksize = self.chunksize;
        let channels = self.channels;
        let target_level = if self.target_level > 0 {
            self.target_level
        } else {
            self.chunksize
        };
        let adjust_period = self.adjust_period;
        let adjust = self.adjust_period > 0.0 && self.enable_rate_adjust;
        let chunksize_clone = chunksize;
        let channels_clone = channels;

        let bits_per_sample = self.sample_format.bits_per_sample() as i32;
        let sample_format = self.sample_format.clone();
        let playback_status_clone = playback_status.clone();
        let handle = thread::Builder::new()
            .name("CpalPlayback".to_string())
            .spawn(move || {
                match open_cpal_playback(host_cfg, &devname, samplerate, channels, &sample_format) {
                    Ok((device, stream_config, _sample_format)) => {
                        match status_channel.send(StatusMessage::PlaybackReady) {
                            Ok(()) => {}
                            Err(_err) => {}
                        }
                        let scalefactor = PrcFmt::new(2.0).powi(bits_per_sample - 1);

                        let (tx_dev, rx_dev) = mpsc::sync_channel(1);
                        let buffer_fill = Arc::new(AtomicUsize::new(0));
                        let buffer_fill_clone = buffer_fill.clone();
                        let mut buffer_avg = countertimer::Averager::new();
                        let mut timer = countertimer::Stopwatch::new();
                        let mut chunk_stats;

                        let stream = match sample_format {
                            SampleFormat::S16LE => {
                                trace!("Build i16 output stream");
                                let mut clipped = 0;
                                let mut sample_queue: VecDeque<i16> =
                                    VecDeque::with_capacity(4 * chunksize_clone * channels_clone);
                                let stream = device.build_output_stream(
                                    &stream_config,
                                    move |mut buffer: &mut [i16], _: &cpal::OutputCallbackInfo| {
                                        //trace!("Playback device requests {} samples", buffer.len());
                                        while sample_queue.len() < buffer.len() {
                                            //trace!("Convert chunk to device format");
                                            let chunk = rx_dev.recv().unwrap();
                                            clipped = chunk_to_queue_int(
                                                &chunk,
                                                &mut sample_queue,
                                                scalefactor,
                                            );
                                        }
                                        write_data_to_device(&mut buffer, &mut sample_queue);
                                        buffer_fill_clone
                                            .store(sample_queue.len(), Ordering::Relaxed);
                                        if clipped > 0 {
                                            playback_status_clone
                                                .write()
                                                .unwrap()
                                                .clipped_samples += clipped;
                                        }
                                    },
                                    move |err| error!("an error occurred on stream: {}", err),
                                );
                                trace!("i16 output stream ready");
                                stream
                            }
                            SampleFormat::FLOAT32LE => {
                                trace!("Build f32 output stream");
                                let mut clipped = 0;
                                let mut sample_queue: VecDeque<f32> =
                                    VecDeque::with_capacity(4 * chunksize_clone * channels_clone);
                                let stream = device.build_output_stream(
                                    &stream_config,
                                    move |mut buffer: &mut [f32], _: &cpal::OutputCallbackInfo| {
                                        //trace!("Playback device requests {} samples", buffer.len());
                                        while sample_queue.len() < buffer.len() {
                                            //trace!("Convert chunk to device format");
                                            let chunk = rx_dev.recv().unwrap();
                                            clipped =
                                                chunk_to_queue_float(&chunk, &mut sample_queue);
                                        }
                                        write_data_to_device(&mut buffer, &mut sample_queue);
                                        buffer_fill_clone
                                            .store(sample_queue.len(), Ordering::Relaxed);
                                        if clipped > 0 {
                                            playback_status_clone
                                                .write()
                                                .unwrap()
                                                .clipped_samples += clipped;
                                        }
                                    },
                                    move |err| error!("an error occurred on stream: {}", err),
                                );
                                trace!("f32 output stream ready");
                                stream
                            }
                            _ => panic!("Unsupported sample format!"),
                        };
                        if let Err(err) = &stream {
                            status_channel
                                .send(StatusMessage::PlaybackError {
                                    message: format!("{}", err),
                                })
                                .unwrap();
                        }
                        barrier.wait();
                        if let Ok(strm) = &stream {
                            match strm.play() {
                                Ok(_) => debug!("Starting playback loop"),
                                Err(err) => status_channel
                                    .send(StatusMessage::PlaybackError {
                                        message: format!("{}", err),
                                    })
                                    .unwrap(),
                            }
                        }
                        loop {
                            match channel.recv() {
                                Ok(AudioMessage::Audio(chunk)) => {
                                    buffer_avg.add_value(
                                        (buffer_fill.load(Ordering::Relaxed) / channels_clone)
                                            as f64,
                                    );
                                    if adjust
                                        && timer.larger_than_millis((1000.0 * adjust_period) as u64)
                                    {
                                        if let Some(av_delay) = buffer_avg.get_average() {
                                            let speed = calculate_speed(
                                                av_delay,
                                                target_level,
                                                adjust_period,
                                                samplerate as u32,
                                            );
                                            timer.restart();
                                            buffer_avg.restart();
                                            debug!(
                                                "Current buffer level {}, set capture rate to {}%",
                                                av_delay,
                                                100.0 * speed
                                            );
                                            status_channel
                                                .send(StatusMessage::SetSpeed { speed })
                                                .unwrap();
                                            playback_status.write().unwrap().buffer_level =
                                                av_delay as usize;
                                        }
                                    }
                                    chunk_stats = chunk.get_stats();
                                    playback_status.write().unwrap().signal_rms =
                                        chunk_stats.rms_db();
                                    playback_status.write().unwrap().signal_peak =
                                        chunk_stats.peak_db();
                                    tx_dev.send(chunk).unwrap();
                                }
                                Ok(AudioMessage::EndOfStream) => {
                                    status_channel.send(StatusMessage::PlaybackDone).unwrap();
                                    break;
                                }
                                Err(err) => {
                                    error!("Message channel error: {}", err);
                                    status_channel.send(StatusMessage::PlaybackDone).unwrap();
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        let send_result = status_channel.send(StatusMessage::PlaybackError {
                            message: format!("{}", err),
                        });
                        if send_result.is_err() {
                            error!("Playback error: {}", err);
                        }
                        barrier.wait();
                    }
                }
            })
            .unwrap();
        Ok(Box::new(handle))
    }
}

fn get_nbr_capture_samples(
    resampler: &Option<Box<dyn Resampler<PrcFmt>>>,
    capture_samples: usize,
    channels: usize,
) -> usize {
    if let Some(resampl) = &resampler {
        let new_capture_samples = resampl.nbr_frames_needed() * channels;
        //trace!(
        //    "Resampler needs {} frames, will read {} samples",
        //    resampl.nbr_frames_needed(),
        //    new_capture_samples
        //);
        new_capture_samples
    } else {
        capture_samples
    }
}

fn write_data_from_device<T>(data: &[T], queue: &mut VecDeque<T>)
where
    T: cpal::Sample,
{
    //trace!("Write data to device");
    for sample in data.iter() {
        queue.push_back(*sample);
    }
}

/// Start a capture thread providing AudioMessages via a channel
impl CaptureDevice for CpalCaptureDevice {
    fn start(
        &mut self,
        channel: mpsc::SyncSender<AudioMessage>,
        barrier: Arc<Barrier>,
        status_channel: mpsc::Sender<StatusMessage>,
        command_channel: mpsc::Receiver<CommandMessage>,
        capture_status: Arc<RwLock<CaptureStatus>>,
    ) -> Res<Box<thread::JoinHandle<()>>> {
        let host_cfg = self.host.clone();
        let devname = self.devname.clone();
        let samplerate = self.samplerate;
        let capture_samplerate = self.capture_samplerate;
        let chunksize = self.chunksize;
        let channels = self.channels;
        let bits_per_sample = self.sample_format.bits_per_sample() as i32;
        let sample_format = self.sample_format.clone();
        let enable_resampling = self.enable_resampling;
        let resampler_conf = self.resampler_conf.clone();
        let async_src = resampler_is_async(&resampler_conf);
        let silence_timeout = self.silence_timeout;
        let silence_threshold = self.silence_threshold;
        let handle = thread::Builder::new()
            .name("CpalCapture".to_string())
            .spawn(move || {
                let mut resampler = if enable_resampling {
                    debug!("Creating resampler");
                    get_resampler(
                        &resampler_conf,
                        channels,
                        samplerate,
                        capture_samplerate,
                        chunksize,
                    )
                } else {
                    None
                };
                match open_cpal_capture(host_cfg, &devname, capture_samplerate, channels, &sample_format) {
                    Ok((device, stream_config, _sample_format)) => {
                        match status_channel.send(StatusMessage::CaptureReady) {
                            Ok(()) => {}
                            Err(_err) => {}
                        }
                        let scalefactor = PrcFmt::new(2.0).powi(bits_per_sample - 1);
                        let (tx_dev_i, rx_dev_i) = mpsc::sync_channel(1);
                        let (tx_dev_f, rx_dev_f) = mpsc::sync_channel(1);
                        let stream = match sample_format {
                            SampleFormat::S16LE => {
                                trace!("Build i16 input stream");
                                let stream = device.build_input_stream(
                                    &stream_config,
                                    move |buffer: &[i16], _: &cpal::InputCallbackInfo| {
                                        //trace!("Capture device provides {} samples", buffer.len());
                                        let mut buffer_copy = Vec::new();
                                        buffer_copy.extend_from_slice(&buffer);
                                        tx_dev_i.send(buffer_copy).unwrap();
                                    },
                                    move |err| error!("an error occurred on stream: {}", err)
                                );
                                trace!("i16 input stream ready");
                                stream
                            },
                            SampleFormat::FLOAT32LE => {
                                trace!("Build f32 input stream");
                                let stream = device.build_input_stream(
                                    &stream_config,
                                    move |buffer: &[f32], _: &cpal::InputCallbackInfo| {
                                        //trace!("Capture device provides {} samples", buffer.len());
                                        let mut buffer_copy = Vec::new();
                                        buffer_copy.extend_from_slice(&buffer);
                                        tx_dev_f.send(buffer_copy).unwrap();
                                    },
                                    move |err| error!("an error occurred on stream: {}", err)
                                );
                                trace!("f32 input stream ready");
                                stream
                            },
                            _ => panic!("Unsupported sample format!"),
                        };
                        if let Err(err) = &stream {
                            status_channel
                                .send(StatusMessage::CaptureError {
                                    message: format!("{}", err),
                                })
                                .unwrap();
                        }
                        barrier.wait();
                        if let Ok(strm) = &stream {
                            match strm.play() {
                                Ok(_) => debug!("Starting capture loop"),
                                Err(err) => status_channel
                                    .send(StatusMessage::CaptureError {
                                        message: format!("{}", err),
                                    })
                                    .unwrap(),
                            }
                        }
                        let chunksize_samples = channels * chunksize;
                        let mut capture_samples = chunksize_samples;
                        let mut sample_queue_i: VecDeque<i16> = VecDeque::with_capacity(2*chunksize*channels);
                        let mut sample_queue_f: VecDeque<f32> = VecDeque::with_capacity(2*chunksize*channels);
                        let mut averager = countertimer::TimeAverage::new();
                        let mut value_range = 0.0;
                        let mut chunk_stats;
                        let mut rate_adjust = 0.0;
                        let mut silence_counter = countertimer::SilenceCounter::new(silence_threshold, silence_timeout, capture_samplerate, chunksize);
                        let mut state = ProcessingState::Running;
                        loop {
                            match command_channel.try_recv() {
                                Ok(CommandMessage::Exit) => {
                                    debug!("Exit message received, sending EndOfStream");
                                    let msg = AudioMessage::EndOfStream;
                                    channel.send(msg).unwrap();
                                    status_channel.send(StatusMessage::CaptureDone).unwrap();
                                    break;
                                }
                                Ok(CommandMessage::SetSpeed { speed }) => {
                                    rate_adjust = speed;
                                    if let Some(resampl) = &mut resampler {
                                        debug!("Adjusting resampler rate to {}", speed);
                                        if async_src {
                                            if resampl.set_resample_ratio_relative(speed).is_err() {
                                                debug!("Failed to set resampling speed to {}", speed);
                                            }
                                        }
                                        else {
                                            warn!("Requested rate adjust of synchronous resampler. Ignoring request.");
                                        }
                                    }
                                }
                                Err(_) => {}
                            };
                            capture_samples = get_nbr_capture_samples(
                                &resampler,
                                capture_samples,
                                channels,
                            );

                            let mut chunk = match sample_format {
                                SampleFormat::S16LE => {
                                    while sample_queue_i.len() < capture_samples {
                                        //trace!("Read message to fill capture buffer");
                                        match rx_dev_i.recv() {
                                            Ok(buf) => {
                                                write_data_from_device(&buf, &mut sample_queue_i);
                                            }
                                            Err(msg) => {
                                                status_channel
                                                    .send(StatusMessage::CaptureError {
                                                        message: format!("{}", msg),
                                                    })
                                                    .unwrap();
                                            }
                                        }
                                    }
                                    queue_to_chunk_int(
                                        &mut sample_queue_i,
                                        capture_samples/channels,
                                        channels,
                                        scalefactor,
                                    )
                                },
                                SampleFormat::FLOAT32LE => {
                                    while sample_queue_f.len() < capture_samples {
                                        //trace!("Read message to fill capture buffer");
                                        match rx_dev_f.recv() {
                                            Ok(buf) => {
                                                write_data_from_device(&buf, &mut sample_queue_f);
                                            }
                                            Err(msg) => {
                                                status_channel
                                                    .send(StatusMessage::CaptureError {
                                                        message: format!("{}", msg),
                                                    })
                                                    .unwrap();
                                            }
                                        }
                                    }
                                    queue_to_chunk_float(
                                        &mut sample_queue_f,
                                        capture_samples/channels,
                                        channels,
                                    )
                                },
                                _ => panic!("Unsupported sample format"),
                            };
                            averager.add_value(capture_samples);
                            if averager.larger_than_millis(capture_status.read().unwrap().update_interval as u64)
                            {
                                let samples_per_sec = averager.get_average();
                                averager.restart();
                                let measured_rate_f = samples_per_sec / channels as f64;
                                trace!(
                                    "Measured sample rate is {} Hz",
                                    measured_rate_f
                                );
                                let mut capt_stat = capture_status.write().unwrap();
                                capt_stat.measured_samplerate = measured_rate_f as usize;
                                capt_stat.signal_range = value_range as f32;
                                capt_stat.rate_adjust = rate_adjust as f32;
                                capt_stat.state = state;
                            }
                            chunk_stats = chunk.get_stats();
                            //trace!("Capture rms {:?}, peak {:?}", chunk_stats.rms_db(), chunk_stats.peak_db());
                            capture_status.write().unwrap().signal_rms = chunk_stats.rms_db();
                            capture_status.write().unwrap().signal_peak = chunk_stats.peak_db();
                            value_range = chunk.maxval - chunk.minval;
                            state = silence_counter.update(value_range);
                            if state == ProcessingState::Running {
                                if let Some(resampl) = &mut resampler {
                                    let new_waves = resampl.process(&chunk.waveforms).unwrap();
                                    chunk.frames = new_waves.iter().map(|w| w.len()).max().unwrap();
                                    chunk.valid_frames = chunk.frames;
                                    chunk.waveforms = new_waves;
                                }
                                let msg = AudioMessage::Audio(chunk);
                                channel.send(msg).unwrap();
                            }
                        }
                        let mut capt_stat = capture_status.write().unwrap();
                        capt_stat.state = ProcessingState::Inactive;
                    }
                    Err(err) => {
                        let send_result = status_channel
                            .send(StatusMessage::CaptureError {
                                message: format!("{}", err),
                            });
                        if send_result.is_err() {
                            error!("Capture error: {}", err);
                        }
                        barrier.wait();
                    }
                }
            })
            .unwrap();
        Ok(Box::new(handle))
    }
}
