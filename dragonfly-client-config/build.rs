/*
 *     Copyright 2024 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::env;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// Commit represents the git commit information.
struct Commit {
    /// hash is the full hash of the commit.
    hash: String,

    /// short_hash is the short hash of the commit.
    short_hash: String,

    /// date is the date of the commit.
    date: String,
}

/// get_commit_from_git returns the git commit information.
fn get_commit_from_git() -> Option<Commit> {
    if !Path::new("../.git").exists() {
        return None;
    }

    let output = match Command::new("git")
        .arg("log")
        .arg("-1")
        .arg("--date=short")
        .arg("--format=%H %h %cd")
        .arg("--abbrev=9")
        .output()
    {
        Ok(output) if output.status.success() => output,
        _ => return None,
    };

    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut parts = stdout.split_whitespace().map(|s| s.to_string());

    Some(Commit {
        hash: parts.next()?,
        short_hash: parts.next()?,
        date: parts.next()?,
    })
}

fn main() {
    // Set the environment variables for the build platform.
    if let Ok(target) = env::var("TARGET") {
        println!("cargo:rustc-env=BUILD_PLATFORM={}", target);
    }

    // Set the environment variables for the build time.
    if let Ok(build_time) = SystemTime::now().duration_since(UNIX_EPOCH) {
        println!("cargo:rustc-env=BUILD_TIMESTAMP={}", build_time.as_secs());
    }

    // Get the commit information from git.
    if let Some(commit) = get_commit_from_git() {
        // Set the environment variables for the git commit.
        println!("cargo:rustc-env=GIT_COMMIT_HASH={}", commit.hash);

        // Set the environment variables for the git commit short.
        println!(
            "cargo:rustc-env=GIT_COMMIT_SHORT_HASH={}",
            commit.short_hash
        );

        // Set the environment variables for the git commit date.
        println!("cargo:rustc-env=GIT_COMMIT_DATE={}", commit.date);
    }
}
