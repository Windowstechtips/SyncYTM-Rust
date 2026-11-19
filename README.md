<div align="center">

# SyncYTM in React

**Listen to YouTube Music together in sync.**

A small, fast, native desktop app written in Rust.

![Rust](https://img.shields.io/badge/Rust-2021-orange?logo=rust&logoColor=white)
![Platform](https://img.shields.io/badge/Platform-Windows-blue?logo=windows&logoColor=white)
![Status](https://img.shields.io/badge/Status-In%20development-yellow)

<br>

[![Download](https://img.shields.io/badge/Download-Latest%20release-2ea44f?style=for-the-badge&logo=github&logoColor=white)](https://github.com/Windowstechtips/SyncYTM-Rust/releases/latest)
&nbsp;

</div>

---

## What is this?

SyncYTM lets you and your friends play the same track at the same moment, no matter where you are. Join a room, pick a song, and everyone hears it together.

This is the Rust rewrite of the original app. The goal was simple: ditch the heavy web wrapper and make something that starts instantly, sips memory, and feels native.

## Features

- **Synced playback** – everyone in the room is synced within 0.5-1 second
- **Native and lightweight** – pure rust without any chromium, Expect RAM usage of 100mb or less
- **Discord Rich Presence** – show what you're listening to on your profile
- **Clean UI** – simple interface built with egui

## Getting started

Grab the latest version using the buttons above

SyncYTM relies on [yt-dlp](https://github.com/yt-dlp/yt-dlp) and [FFmpeg](https://ffmpeg.org) to fetch and process audio. If either one isn't installed on your system, the app will download it for you on first launch.

## Built with

| Component          | Crate                                                            |
| ------------------ | ---------------------------------------------------------------- |
| UI                 | [`eframe` / `egui`](https://github.com/emilk/egui)               |
| Async runtime      | [`tokio`](https://tokio.rs)                                      |
| Networking         | `reqwest`, `tokio-tungstenite`                                   |
| Audio              | [`cpal`](https://github.com/RustAudio/cpal)                      |
| Discord presence   | `discord-rich-presence`                                          |
| Audio extraction   | [`yt-dlp`](https://github.com/yt-dlp/yt-dlp)                     |

## Contributing

Found a bug or have an idea? Open an [issue](https://github.com/Windowstechtips/SyncYTM-Rust/issues) or send a pull request.

## Disclaimer

SyncYTM is a personal project and isn't affiliated with YouTube, Google, or Discord.

## License

No license has been added yet. Until one is, all rights are reserved by the author.
