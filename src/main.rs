#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        {
            eprintln!($($arg)*);
        }
    };
}

mod app;
mod audio;
mod config;
mod deps;
mod discord;
mod models;
mod realtime;
mod supabase;
mod youtube;

use app::SyncYtmApp;
use eframe::egui::{self, Color32, Rounding, Stroke, Vec2, Visuals};

fn configure_fonts(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();
    let font_candidates = [
        r"C:\Windows\Fonts\msgothic.ttc",
        r"C:\Windows\Fonts\YuGothR.ttc",
        r"C:\Windows\Fonts\YuGothM.ttc",
        r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\meiryo.ttc",
    ];
    for path in font_candidates {
        if let Ok(data) = std::fs::read(path) {
            fonts.font_data.insert(
                "cjk_fallback".to_owned(),
                egui::FontData::from_owned(data),
            );
            if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Proportional) {
                family.push("cjk_fallback".to_owned());
            }
            if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Monospace) {
                family.push("cjk_fallback".to_owned());
            }
            break;
        }
    }
    ctx.set_fonts(fonts);
}

fn configure_visuals(ctx: &egui::Context) {
    let mut visuals = Visuals::dark();

    let bg_base          = Color32::from_rgb(13, 13, 15);
    let bg_panel         = Color32::from_rgb(18, 18, 24);
    let bg_card          = Color32::from_rgb(24, 24, 32);
    let bg_hover         = Color32::from_rgb(28, 28, 40);
    let bg_active_widget = Color32::from_rgb(50, 20, 22);
    let border           = Color32::from_rgb(40, 40, 52);
    let accent           = Color32::from_rgb(232, 54, 61);
    let text_pri         = Color32::from_rgb(240, 240, 245);
    let text_muted       = Color32::from_rgb(120, 120, 135);

    visuals.window_fill       = bg_base;
    visuals.panel_fill        = bg_panel;
    visuals.faint_bg_color    = bg_card;
    visuals.extreme_bg_color  = bg_base;
    visuals.code_bg_color     = bg_card;
    visuals.window_rounding   = Rounding::same(8.0);
    visuals.window_stroke     = Stroke::new(1.0_f32, border);
    visuals.popup_shadow      = egui::epaint::Shadow::NONE;
    visuals.hyperlink_color   = accent;
    visuals.selection.bg_fill = Color32::from_rgba_premultiplied(232, 54, 61, 60);
    visuals.selection.stroke  = Stroke::new(1.0_f32, accent);
    visuals.override_text_color = Some(text_pri);

    let r = Rounding::same(4.0);

    visuals.widgets.noninteractive.rounding  = r;
    visuals.widgets.noninteractive.bg_fill   = bg_panel;
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0_f32, border);
    visuals.widgets.noninteractive.fg_stroke = Stroke::new(1.0_f32, text_muted);

    visuals.widgets.inactive.rounding  = r;
    visuals.widgets.inactive.bg_fill   = bg_card;
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0_f32, border);
    visuals.widgets.inactive.fg_stroke = Stroke::new(1.0_f32, text_pri);

    visuals.widgets.hovered.rounding  = r;
    visuals.widgets.hovered.bg_fill   = bg_hover;
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.0_f32, accent);
    visuals.widgets.hovered.fg_stroke = Stroke::new(1.5_f32, text_pri);

    visuals.widgets.active.rounding  = r;
    visuals.widgets.active.bg_fill   = bg_active_widget;
    visuals.widgets.active.bg_stroke = Stroke::new(1.0_f32, accent);
    visuals.widgets.active.fg_stroke = Stroke::new(2.0_f32, text_pri);

    visuals.widgets.open.rounding  = r;
    visuals.widgets.open.bg_fill   = bg_card;
    visuals.widgets.open.bg_stroke = Stroke::new(1.0_f32, accent);
    visuals.widgets.open.fg_stroke = Stroke::new(1.5_f32, text_pri);

    ctx.set_visuals(visuals);

    let mut style = (*ctx.style()).clone();
    style.spacing.item_spacing    = Vec2::new(6.0, 4.0);
    style.spacing.button_padding  = Vec2::new(10.0, 5.0);
    style.spacing.window_margin   = egui::Margin::same(12.0);
    style.spacing.indent          = 12.0;
    style.spacing.scroll.bar_width = 6.0;
    ctx.set_style(style);
}

fn main() -> eframe::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to initialize Tokio runtime");
    let _enter = rt.enter();

    let native_options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([980.0, 680.0])
            .with_resizable(false)
            .with_maximize_button(false)
            .with_title("SyncYTM - Native Audio Client"),
        ..Default::default()
    };

    eframe::run_native(
        "SyncYTM",
        native_options,
        Box::new(|cc| {
            configure_fonts(&cc.egui_ctx);
            configure_visuals(&cc.egui_ctx);
            let app = SyncYtmApp::new();
            Ok(Box::new(app))
        }),
    )
}
