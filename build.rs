use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    slint_build::compile("ui/app_window.slint").expect("Failed to compile Slint UI definition");

    // Automatically locate and copy tdjson.dll to target directory on Windows
    #[cfg(windows)]
    {
        if let Ok(out_dir) = env::var("OUT_DIR") {
            let out_path = PathBuf::from(out_dir);
            // OUT_DIR is target/debug/build/nvgram-xxx/out
            // Target dir is target/debug
            if let Some(target_dir) = out_path.ancestors().nth(3) {
                if let Some(build_dir) = out_path.parent().and_then(|p| p.parent()) {
                    if let Ok(entries) = fs::read_dir(build_dir) {
                        for entry in entries.flatten() {
                            let dll_path = entry.path().join("out/tdlib/bin/tdjson.dll");
                            if dll_path.exists() {
                                let dest = target_dir.join("tdjson.dll");
                                let _ = fs::copy(&dll_path, dest);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}
