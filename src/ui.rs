//! Terminal progress and logging. Progress goes to stderr as one spinner per phase that becomes a
//! permanent `✓ phase  summary · time` line. Without a terminal (CI, pipes) nothing is drawn and
//! each finished phase is logged instead. Logs are printed above the spinners so they don't tear.

use std::fmt::Display;
use std::io::IsTerminal;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use log::{Level, LevelFilter, Log, Metadata, Record};

static PROGRESS: OnceLock<MultiProgress> = OnceLock::new();

fn progress() -> &'static MultiProgress {
    PROGRESS.get_or_init(|| MultiProgress::with_draw_target(ProgressDrawTarget::stderr()))
}

/// Whether live progress is drawn (stderr is a terminal).
fn interactive() -> bool {
    std::io::stderr().is_terminal()
}

/// One unit of work shown as a spinner, e.g. "us-west-2 · launch templates".
pub(crate) struct Phase {
    bar: ProgressBar,
    label: String,
    started: Instant,
    finished: bool,
}

impl Phase {
    pub(crate) fn start(label: impl Into<String>) -> Self {
        let label = label.into();
        let bar = progress().add(ProgressBar::new_spinner());
        bar.set_style(
            ProgressStyle::with_template(
                "{spinner:.cyan} {prefix:.bold} {msg:.dim} {elapsed:.dim}",
            )
            .expect("valid template")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
        );
        bar.set_prefix(label.clone());
        bar.enable_steady_tick(Duration::from_millis(100));
        Self { bar, label, started: Instant::now(), finished: false }
    }

    /// A phase with a known amount of work; shows `done/total`.
    pub(crate) fn with_total(label: impl Into<String>, total: usize) -> Self {
        let phase = Self::start(label);
        phase.set_total(total);
        phase
    }

    /// Switches to `done/total` once the amount of work is known.
    pub(crate) fn set_total(&self, total: usize) {
        self.bar.set_length(total as u64);
        self.bar.set_style(
            ProgressStyle::with_template(
                "{spinner:.cyan} {prefix:.bold} {pos}/{len} {msg:.dim} {elapsed:.dim}",
            )
            .expect("valid template")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
        );
    }

    pub(crate) fn inc(&self) {
        self.bar.inc(1);
    }

    pub(crate) fn message(&self, message: impl Into<String>) {
        self.bar.set_message(message.into());
    }

    pub(crate) fn finish(self, summary: impl Display) {
        self.close(console::style("✓").green().for_stderr(), summary);
    }

    pub(crate) fn warn(self, summary: impl Display) {
        self.close(console::style("⚠").yellow().for_stderr(), summary);
    }

    pub(crate) fn fail(self, summary: impl Display) {
        self.close(console::style("✗").red().for_stderr(), summary);
    }

    /// For work that didn't run (e.g. a skipped reference source).
    pub(crate) fn skip(self, summary: impl Display) {
        self.close(console::style("–").dim().for_stderr(), summary);
    }

    fn close(mut self, mark: console::StyledObject<&str>, summary: impl Display) {
        self.finished = true;
        let elapsed = format_duration(self.started.elapsed());
        if interactive() {
            self.bar.set_style(ProgressStyle::with_template("{msg}").expect("valid template"));
            self.bar.finish_with_message(format!(
                "{mark} {} {summary} {}",
                console::style(&self.label).bold().for_stderr(),
                console::style(format!("· {elapsed}")).dim().for_stderr()
            ));
        } else {
            self.bar.finish_and_clear();
            log::info!("{} {}: {summary} ({elapsed})", mark, self.label);
        }
    }
}

impl Drop for Phase {
    fn drop(&mut self) {
        if !self.finished {
            self.bar.finish_and_clear();
        }
    }
}

/// Prints a block (e.g. a region summary) to stdout without tearing the spinners.
pub(crate) fn print_stdout(text: &str) {
    progress().suspend(|| print!("{text}"));
}

/// `86849` -> `86,849`.
pub(crate) fn thousands(value: usize) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs_f64();
    if seconds < 1.0 {
        format!("{}ms", duration.as_millis())
    } else if seconds < 60.0 {
        format!("{seconds:.1}s")
    } else {
        format!("{}m{:02}s", duration.as_secs() / 60, duration.as_secs() % 60)
    }
}

/// Logs to stderr, above any live spinners. `RUST_LOG` sets the level (error, warn, info, debug,
/// trace; default info). Only this crate's records are shown.
struct Logger {
    level: LevelFilter,
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= self.level && metadata.target().starts_with(env!("CARGO_CRATE_NAME"))
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let level = match record.level() {
            Level::Error => console::style("error").red().bold(),
            Level::Warn => console::style("warn ").yellow().bold(),
            Level::Info => console::style("info ").cyan(),
            Level::Debug => console::style("debug").dim(),
            Level::Trace => console::style("trace").dim(),
        }
        .for_stderr();
        let line = format!("{level} {}", record.args());
        progress().suspend(|| eprintln!("{line}"));
    }

    fn flush(&self) {}
}

pub(crate) fn parse_log_level(raw: Option<&str>) -> LevelFilter {
    raw.and_then(|value| value.trim().parse().ok()).unwrap_or(LevelFilter::Info)
}

pub(crate) fn init_logging() -> Result<(), log::SetLoggerError> {
    let level = parse_log_level(std::env::var("RUST_LOG").ok().as_deref());
    // Leaked once at startup: `set_logger` needs a `'static` logger and doesn't need log's
    // `alloc` feature like `set_boxed_logger` does.
    log::set_logger(Box::leak(Box::new(Logger { level })))?;
    log::set_max_level(level);
    Ok(())
}
