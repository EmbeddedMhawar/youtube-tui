use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::{env, error::Error, io};
use tui_additions::framework::{Framework, State};
use youtube_tui_ai::{exit, global::functions::text_command, init, run};

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>().join(" ");

    if let Some(s) = text_command(&args) {
        println!("{s}");
        return Ok(());
    }

    let state = State(Vec::new());
    let mut framework = Framework::new(state);

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Use spawn_blocking for the main loop if needed, but standard run should work
    let res = init(
        &mut framework,
        &mut terminal,
        if args.is_empty() { None } else { Some(&args) },
    );

    let res = if res.is_ok() {
        run(&mut terminal, &mut framework)
    } else {
        Err(res.err().unwrap())
    };

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    let exit_res = exit(&mut framework);

    res?;
    exit_res?;

    Ok(())
}
