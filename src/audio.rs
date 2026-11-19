use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use std::io::Read;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

pub struct AudioEngine {
    pub stream: cpal::Stream,
    pub controller: AudioController,
}

#[derive(Clone)]
pub struct AudioController {
    state: Arc<AudioState>,
    #[allow(dead_code)]
    sample_rate: u32,
}

unsafe impl Send for AudioController {}
unsafe impl Sync for AudioController {}

struct AudioState {
    is_playing: AtomicBool,
    volume: Mutex<f32>,
    start_time_bits: AtomicU64,
    samples_played: AtomicU64,
    #[allow(dead_code)]
    duration_ms: AtomicU64,
    is_finished: AtomicBool,
    generation: AtomicU64,
    flush_gen: AtomicU64,
    active_url: Mutex<Option<String>>,
    pending_seek_sec: Mutex<Option<f64>>,
    channels: AtomicU64,
}

impl AudioEngine {
    pub fn new() -> Result<Self, String> {
        let host = cpal::default_host();
        let device = host
            .default_output_device()
            .ok_or_else(|| "No default audio output device found".to_string())?;

        let supported = device
            .default_output_config()
            .map_err(|e| format!("No supported output config: {}", e))?;

        let sample_rate = supported.sample_rate().0;
        let channels = supported.channels() as usize;

        let config = cpal::StreamConfig {
            channels: channels as u16,
            sample_rate: cpal::SampleRate(sample_rate),
            buffer_size: cpal::BufferSize::Default,
        };

        // 2.0s buffer for jitter-free streaming without underruns
        let buf_size = sample_rate as usize * channels * 2;
        let (tx, rx) = crossbeam_channel::bounded::<f32>(buf_size);

        let state = Arc::new(AudioState {
            is_playing: AtomicBool::new(false),
            volume: Mutex::new(1.0),
            start_time_bits: AtomicU64::new(0.0_f64.to_bits()),
            samples_played: AtomicU64::new(0),
            duration_ms: AtomicU64::new(0),
            is_finished: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            flush_gen: AtomicU64::new(0),
            active_url: Mutex::new(None),
            pending_seek_sec: Mutex::new(None),
            channels: AtomicU64::new(channels as u64),
        });

        let state_clone = state.clone();

        let stream = device
            .build_output_stream(
                &config,
                move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
                    static LOCAL_FLUSH: AtomicU64 = AtomicU64::new(0);

                    // If a seek or new song occurred, instantly drain any stale buffered audio
                    let cur_flush = state_clone.flush_gen.load(Ordering::Relaxed);
                    if cur_flush != LOCAL_FLUSH.load(Ordering::Relaxed) {
                        LOCAL_FLUSH.store(cur_flush, Ordering::Relaxed);
                        while rx.try_recv().is_ok() {}
                    }

                    let playing = state_clone.is_playing.load(Ordering::Relaxed);
                    let vol = *state_clone.volume.lock().unwrap_or_else(|e| e.into_inner());

                    if !playing {
                        for sample in data.iter_mut() {
                            *sample = 0.0;
                        }
                        return;
                    }

                    let mut samples_consumed = 0u64;
                    for sample in data.iter_mut() {
                        match rx.try_recv() {
                            Ok(val) => {
                                *sample = val * vol;
                                samples_consumed += 1;
                            }
                            Err(_) => {
                                *sample = 0.0;
                            }
                        }
                    }

                    if samples_consumed > 0 {
                        state_clone
                            .samples_played
                            .fetch_add(samples_consumed, Ordering::Relaxed);
                    }
                },
                |err| eprintln!("Audio stream error: {}", err),
                None,
            )
            .map_err(|e| format!("Failed to build audio output stream: {}", e))?;

        stream
            .play()
            .map_err(|e| format!("Failed to play audio stream: {}", e))?;

        let feeder_state = state.clone();
        Self::spawn_feeder_worker(feeder_state, tx, sample_rate, channels as u16);

        Ok(Self {
            stream,
            controller: AudioController { state, sample_rate },
        })
    }

    fn spawn_feeder_worker(
        state: Arc<AudioState>,
        tx: crossbeam_channel::Sender<f32>,
        sample_rate: u32,
        channels: u16,
    ) {
        thread::spawn(move || {
            let sr_str = sample_rate.to_string();
            let ch_str = channels.to_string();
            let mut last_gen = 0u64;

            loop {
                thread::sleep(std::time::Duration::from_millis(10));

                let cur_gen = state.generation.load(Ordering::Relaxed);
                if cur_gen == 0 || cur_gen == last_gen {
                    continue;
                }
                last_gen = cur_gen;

                let maybe_url = state.active_url.lock().unwrap().clone();
                let Some(url) = maybe_url else {
                    continue;
                };

                let ffmpeg = crate::deps::find_ffmpeg().unwrap_or_else(|| PathBuf::from("ffmpeg"));

                let maybe_seek = state.pending_seek_sec.lock().unwrap().take();
                let is_seek = maybe_seek.is_some();
                let start_sec = if let Some(s) = maybe_seek {
                    s
                } else {
                    f64::from_bits(state.start_time_bits.load(Ordering::Relaxed))
                };

                // Fast-startup, smooth streaming flags:
                // Input seeking (-ss before -i) + 64KB probesize for 300ms startup
                let child = crate::deps::create_command(&ffmpeg)
                    .args([
                        "-probesize",
                        "65536",
                        "-analyzeduration",
                        "200000",
                        "-reconnect",
                        "1",
                        "-reconnect_streamed",
                        "1",
                        "-reconnect_delay_max",
                        "5",
                        "-ss",
                        &format!("{:.2}", start_sec),
                        "-i",
                        &url,
                        "-vn",
                        "-f",
                        "s16le",
                        "-ac",
                        &ch_str,
                        "-ar",
                        &sr_str,
                        "-loglevel",
                        "error",
                        "-",
                    ])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .spawn();

                let mut child = match child {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("Failed to spawn ffmpeg: {}", e);
                        thread::sleep(std::time::Duration::from_millis(500));
                        continue;
                    }
                };

                let mut stdout = match child.stdout.take() {
                    Some(s) => s,
                    None => {
                        let _ = child.kill();
                        continue;
                    }
                };

                state.is_finished.store(false, Ordering::Relaxed);
                let mut is_first_chunk = true;

                let mut buf = [0u8; 4096];
                loop {
                    // Check if generation changed (i.e. play/seek/stop requested)
                    if state.generation.load(Ordering::Relaxed) != cur_gen {
                        break;
                    }

                    match stdout.read(&mut buf) {
                        Ok(0) => {
                            state.is_finished.store(true, Ordering::Relaxed);
                            break;
                        }
                        Ok(n) => {
                            if is_first_chunk {
                                is_first_chunk = false;
                                if is_seek {
                                    // Seamless seek: precisely when the new position is decoded,
                                    // flush old buffer samples and snap audio clock
                                    state.start_time_bits.store(start_sec.to_bits(), Ordering::Relaxed);
                                    state.samples_played.store(0, Ordering::Relaxed);
                                    state.flush_gen.fetch_add(1, Ordering::Relaxed);
                                }
                            }

                            let mut abort = false;
                            for chunk in buf[..n].chunks_exact(2) {
                                let s = i16::from_le_bytes([chunk[0], chunk[1]]);
                                let f = (s as f32) / 32768.0;

                                let mut val = f;
                                loop {
                                    if state.generation.load(Ordering::Relaxed) != cur_gen {
                                        abort = true;
                                        break;
                                    }
                                    match tx.send_timeout(val, std::time::Duration::from_millis(30)) {
                                        Ok(_) => {
                                            break;
                                        }
                                        Err(crossbeam_channel::SendTimeoutError::Timeout(v)) => {
                                            val = v;
                                        }
                                        Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => {
                                            abort = true;
                                            break;
                                        }
                                    }
                                }
                                if abort {
                                    break;
                                }
                            }
                            if abort {
                                break;
                            }
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }

                // Clean up process strictly on this background thread
                let _ = child.kill();
                let _ = child.wait();
            }
        });
    }
}

impl AudioController {
    pub fn play(&self, url: &str, start_sec: f64) {
        self.state
            .start_time_bits
            .store(start_sec.to_bits(), Ordering::Relaxed);
        self.state.samples_played.store(0, Ordering::Relaxed);
        *self.state.pending_seek_sec.lock().unwrap() = None;
        *self.state.active_url.lock().unwrap() = Some(url.to_string());
        self.state.is_playing.store(true, Ordering::Relaxed);
        self.state.is_finished.store(false, Ordering::Relaxed);
        // Increment flush_gen to dump any stale audio in ring buffer
        self.state.flush_gen.fetch_add(1, Ordering::Relaxed);
        // Increment generation to trigger feeder thread without blocking UI
        self.state.generation.fetch_add(1, Ordering::Relaxed);
    }

    pub fn seek(&self, target_sec: f64) {
        let current_url = self.state.active_url.lock().unwrap().clone();
        if current_url.is_some() {
            // Seamless seek: defer ring-buffer flush until new FFmpeg chunk arrives
            *self.state.pending_seek_sec.lock().unwrap() = Some(target_sec);
            self.state.generation.fetch_add(1, Ordering::Relaxed);
        } else {
            self.state
                .start_time_bits
                .store(target_sec.to_bits(), Ordering::Relaxed);
            self.state.samples_played.store(0, Ordering::Relaxed);
        }
    }

    pub fn pause(&self) {
        self.state.is_playing.store(false, Ordering::Relaxed);
    }

    pub fn resume(&self) {
        self.state.is_playing.store(true, Ordering::Relaxed);
    }

    pub fn reset_finished(&self) {
        self.state.is_finished.store(false, Ordering::Relaxed);
    }

    pub fn stop(&self) {
        self.state.is_playing.store(false, Ordering::Relaxed);
        self.state.is_finished.store(false, Ordering::Relaxed);
        *self.state.active_url.lock().unwrap() = None;
        *self.state.pending_seek_sec.lock().unwrap() = None;
        self.state
            .start_time_bits
            .store(0.0_f64.to_bits(), Ordering::Relaxed);
        self.state.samples_played.store(0, Ordering::Relaxed);
        self.state.flush_gen.fetch_add(1, Ordering::Relaxed);
        self.state.generation.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_volume(&self, vol: f32) {
        *self.state.volume.lock().unwrap() = vol.clamp(0.0, 1.0);
    }

    pub fn get_volume(&self) -> f32 {
        *self.state.volume.lock().unwrap()
    }

    pub fn get_time_sec(&self) -> f64 {
        let base = f64::from_bits(self.state.start_time_bits.load(Ordering::Relaxed));
        let samples = self.state.samples_played.load(Ordering::Relaxed) as f64;
        let sr = self.sample_rate as f64;
        let ch = self.state.channels.load(Ordering::Relaxed) as f64;
        if sr > 0.0 && ch > 0.0 {
            base + (samples / (sr * ch))
        } else {
            base
        }
    }

    #[allow(dead_code)]
    pub fn is_playing(&self) -> bool {
        self.state.is_playing.load(Ordering::Relaxed)
    }

    pub fn is_finished(&self) -> bool {
        self.state.is_finished.load(Ordering::Relaxed)
    }
}
