#[cfg(feature = "mpv")]
use crate::global::functions::secs_display_string;
use crate::{config::*, global::structs::*};
use ratatui::{
    layout::Rect,
    style::Style,
    widgets::{Block, Borders, Paragraph},
};
use tui_additions::framework::*;

/// a message bar item, contains no fields because the message is taken from `data.global.Message`
#[derive(Clone, Copy, Default)]
pub struct MessageBar;

impl FrameworkItem for MessageBar {
    fn render(
        &mut self,
        frame: &mut ratatui::Frame,
        framework: &mut tui_additions::framework::FrameworkClean,
        area: ratatui::layout::Rect,
        popup_render: bool,
        _info: tui_additions::framework::ItemInfo,
    ) {
        if popup_render {
            return;
        }

        let ai_progress_res = SHARED_AI_PROGRESS.lock();
        let is_ai_active = ai_progress_res.as_ref().map(|p| p.active).unwrap_or(false);

        #[cfg(feature = "mpv")]
        {
            if let Some(mpv) = framework.data.global.get::<MpvWrapper>() {
                if !is_ai_active && Self::is_mpv_render(framework) {
                    let mut label = mpv.property("media-title".to_string()).unwrap_or_else(|| "Unknown".to_string());
                    if let Some((name, ext)) = label.rsplit_once('.') {
                        if ext.len() < 5
                            && name.len() > 13
                            && &name[name.len() - 1..name.len()] == "]"
                            && name[name.len() - 12..name.len() - 1]
                                .chars()
                                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
                            && &name[name.len() - 13..name.len() - 12] == "["
                        {
                            label = name[0..name.len() - 13].to_string();
                        }
                    }
                    let duration = mpv
                        .property("duration".to_string())
                        .unwrap_or_default()
                        .parse::<f64>()
                        .unwrap_or(0.0) as u64;
                    let duration_s = secs_display_string(duration as u32);
                    let playerhead = mpv
                        .property("time-pos".to_string())
                        .unwrap_or_default()
                        .parse::<f64>()
                        .unwrap_or(0.0) as u64;
                    let mut playerhead_s = secs_display_string(playerhead as u32);
                    if playerhead_s.len() != duration_s.len() {
                        playerhead_s = format!(
                            "{}{playerhead_s}",
                            " ".repeat(duration_s.len() - playerhead_s.len())
                        );
                    }
                    let percentage = if duration > 0 { (playerhead * 100 / duration).to_string() } else { "0".to_string() };

                    let right_chunk = format!(
                        "{playerhead_s}/{duration_s} {}[{percentage}%]",
                        " ".repeat(3 - percentage.len())
                    );
                    let left_chunk = format!("[Now Playing]: {label}");
                    let length = area.width as usize - 2;
                    let total_len = right_chunk.len() + left_chunk.len();

                    *framework.data.global.get_mut::<Message>().unwrap() =
                        Message::Mpv(if length > total_len + 6 {
                            let mut seeker_len = length - total_len - 4;
                            let seeker_pad = if seeker_len > 10 { seeker_len / 10 } else { 0 };
                            seeker_len -= seeker_pad * 2;
                            let seeker_pos = if duration > 0 { (seeker_len - 1) * playerhead as usize / duration as usize } else { 0 };
                            format!(
                                "{left_chunk} {}├{}-{}┤{} {right_chunk}",
                                " ".repeat(seeker_pad),
                                "─".repeat(seeker_pos),
                                "─".repeat(seeker_len - seeker_pos - 1),
                                " ".repeat(seeker_pad)
                            )
                        } else if length > total_len + 3 {
                            format!(
                                "{left_chunk}{}{right_chunk}",
                                " ".repeat(length - total_len)
                            )
                        } else if length > 19 {
                            left_chunk
                        } else {
                            String::from("Not enough width")
                        });
                }
            }
        }

        let message = framework.data.global.get::<Message>().unwrap();
        let appearance = framework.data.global.get::<AppearanceConfig>().unwrap();

        // the Option<TextList> that is Some if keys were captured for entering command
        let command_capture = &framework
            .data
            .global
            .get::<Status>()
            .unwrap()
            .command_capture;

        // display with different border style according to type of message and config
        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(appearance.borders)
            .border_style(Style::default().fg(if command_capture.is_some() {
                appearance.colors.command_capture
            } else {
                match message {
                    Message::None => appearance.colors.outline,
                    Message::Success(_) => appearance.colors.message_success_outline,
                    Message::Error(_) => appearance.colors.message_error_outline,
                    Message::Message(_) | Message::Mpv(_) => appearance.colors.message_outline,
                }
            }));

        // if keys are captured, render the textlist instead of the message text, and exits the
        // function
        if let Some(textfield) = command_capture {
            let paragraph = Paragraph::new(":").block(block);
            frame.render_widget(paragraph, area);
            let mut textfield = textfield.clone();
            textfield.set_width(area.width - 3);
            let _ = textfield.update();
            frame.render_widget(
                textfield,
                Rect::new(area.x + 2, area.y + 1, area.width - 3, 1),
            );

            return;
        }

        let content = if is_ai_active {
            if let Ok(guard) = ai_progress_res {
                let eta = guard.eta_seconds.map(|s| format!(" (ETA: {}s)", s)).unwrap_or_default();
                let ratio = guard.ratio.map(|r| format!(" | Speed: {:.2}x", r)).unwrap_or_default();
                let progress = if guard.total_chunks > 0 {
                    format!(" | Chunk {} of {}", guard.current_chunk, guard.total_chunks)
                } else {
                    String::new()
                };

                let combined_label = if !guard.worker_label.is_empty() && guard.label != guard.worker_label {
                    format!("{} | {}", guard.label, guard.worker_label)
                } else {
                    guard.label.clone()
                };

                format!("[AI]: {}{}{}{}", combined_label, progress, eta, ratio)
            } else {
                String::from("AI Progress Lock Error")
            }
        } else {
            message.to_string(
                &framework
                    .data
                    .global
                    .get::<MainConfig>()
                    .unwrap()
                    .message_bar_default,
            )
        };

        let paragraph = Paragraph::new(content).block(block);
        frame.render_widget(paragraph, area);
    }

    fn selectable(&self) -> bool {
        false
    }
}

#[cfg(feature = "mpv")]
impl MessageBar {
    pub fn is_mpv_render(framework: &FrameworkClean) -> bool {
        framework.data.global.get::<MpvWrapper>().unwrap().playing()
            && matches!(
                framework.data.global.get::<Message>().unwrap(),
                Message::Mpv(_) | Message::None
            )
            && framework
                .data
                .global
                .get::<Status>()
                .unwrap()
                .command_capture
                .is_none()
    }
}
