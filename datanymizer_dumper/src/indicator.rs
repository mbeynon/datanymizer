use indicatif::{HumanDuration, ProgressBar, ProgressStyle};
use std::time::Duration;

pub trait Indicator {
    fn start_pb(&self, _size: u64, _prefix: &str) {}
    fn inc_pb(&self, _i: u64) {}
    fn finish_pb(&self, _name: &str, _duration: Duration) {}

    fn start_pb_stream(&self, _cur: u64, _tot: u64, _name: &str, _msg: &str) {}
    fn inc_pb_stream(&self, _i: u64) {}
    fn finish_pb_stream(&mut self, _i: u64, _duration: Duration) {}

    fn debug_msg(&self, _msg: &str) {}
}

pub struct SilentIndicator;

impl Indicator for SilentIndicator {}

pub struct ConsoleIndicator {
    pb: ProgressBar,
}

impl ConsoleIndicator {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for ConsoleIndicator {
    fn default() -> Self {
        let pb = ProgressBar::new(0);
        Self { pb }
    }
}

impl Indicator for ConsoleIndicator {
    fn start_pb(&self, size: u64, name: &str) {
        let delta = size / 100;
        self.pb.set_length(size);
        self.pb.set_draw_delta(delta);
        self.pb.set_prefix(name);
        self.pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[Dumping: {prefix}] [|{bar:50}|] {pos} of {len} rows [{percent}%] ({eta})",
                )
                .progress_chars("#>-"),
        );
    }

    fn inc_pb(&self, i: u64) {
        self.pb.inc(i);
    }

    fn finish_pb(&self, name: &str, duration: Duration) {
        self.pb.finish();
        self.pb.reset();

        self.debug_msg(
            format!(
                "[Dumping: {}] Finished in {}",
                name,
                HumanDuration(duration)
            )
            .as_str(),
        );
    }

    fn start_pb_stream(&self, cur: u64, tot: u64, name: &str, msg: &str) {
        self.pb.set_style(
            ProgressStyle::default_spinner()
                .template("{prefix:.bold.dim}  {spinner}  {wide_msg}")
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        );
        let prefix = format!("[{}/{}] [Dumping: {}]", cur, tot, name);
        self.pb.set_prefix(&prefix);
        self.pb.set_position(0);
        self.pb.set_message(msg);
    }

    fn inc_pb_stream(&self, i: u64) {
        let msg = format!("{} rows", i);
        self.pb.inc(i);
        self.pb.set_message(&msg);
    }

    fn finish_pb_stream(&mut self, i: u64, duration: Duration) {
        let msg = format!("{} rows processed in ({})", i, HumanDuration(duration));
        self.pb.finish_with_message(&msg);
        self.pb.reset();
        self.pb.set_prefix("");
        self.pb.set_message("");
        self.debug_msg("");  // hack to preserve the last line display
    }

    fn debug_msg(&self, msg: &str) {
        println!("{}", msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // just test that there is no panic
    mod console_indicator {
        use super::*;

        #[test]
        fn debug_msg() {
            ConsoleIndicator::new().debug_msg("some message");
        }

        #[test]
        fn pb_start_finish() {
            let ci = ConsoleIndicator::new();
            ci.start_pb(100, "name");
            ci.finish_pb("name", Duration::new(1, 0));
        }

        #[test]
        fn pb_some_progress() {
            let ci = ConsoleIndicator::new();
            ci.start_pb(100, "name");
            ci.inc_pb(1);
            ci.inc_pb(10);
            ci.finish_pb("name", Duration::new(1, 0));
        }

        #[test]
        fn pb_overflow_progress() {
            let ci = ConsoleIndicator::new();
            ci.start_pb(100, "name");
            ci.inc_pb(1);
            ci.inc_pb(10);
            ci.inc_pb(100);
            ci.finish_pb("name", Duration::new(1, 0));
        }
    }
}
