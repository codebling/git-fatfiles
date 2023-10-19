use std::{
    collections::HashMap,
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
};

use bytesize::ByteSize;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt()]
pub struct Opt {
    #[structopt(
        short,
        long,
        help("Show the size of directories based on files committed in them.")
    )]
    pub directories: bool,

    #[structopt(help("Optional: only show the size info about certain paths."))]
    pub paths: Vec<String>,
}

/// The paths list is a filter. If empty, there is no filtering.
/// Returns a map of object ID -> filename.
fn get_revs_for_paths(paths: Vec<String>) -> HashMap<String, PathBuf> {
    let mut process = Command::new("git");
    let mut process = process.arg("rev-list").arg("--all").arg("--objects");

    if !paths.is_empty() {
        process = process.arg("--").args(paths);
    };

    let output = process
        .output()
        .expect("Failed to execute command git rev-list.");

    let mut id_map = HashMap::new();
    for line in io::Cursor::new(output.stdout).lines() {
        if let Some((k, v)) = line
            .expect("Failed to get line from git command output.")
            .split_once(' ')
        {
            id_map.insert(k.to_owned(), PathBuf::from(v));
        }
    }
    id_map
}

/// Returns a map of object ID to size.
fn get_sizes_of_objects(ids: Vec<&String>) -> HashMap<String, u64> {
    let mut process = Command::new("git")
        .arg("cat-file")
        .arg("--batch-check=%(objectname) %(objecttype) %(objectsize:disk)")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to execute command git cat-file.");
    let mut stdin = process.stdin.expect("Could not open child stdin.");

    let ids: Vec<String> = ids.into_iter().cloned().collect(); // copy data for thread

    // Stdin will block when the output buffer gets full, so it needs to be written
    // in a thread:
    let write_thread = thread::spawn(|| {
        for obj_id in ids {
            writeln!(stdin, "{}", obj_id).expect("Could not write to child stdin");
        }
        drop(stdin);
    });

    let output = process
        .stdout
        .take()
        .expect("Could not get output of command git cat-file.");

    let mut id_map = HashMap::new();
    for line in BufReader::new(output).lines() {
        let line = line.expect("Failed to get line from git command output.");

        let line_split: Vec<&str> = line.split(' ').collect();

        // skip non-blob objects
        if let [id, "blob", size] = &line_split[..] {
            id_map.insert(
                id.to_string(),
                size.parse::<u64>().expect("Could not convert size to int."),
            );
        };
    }
    write_thread.join().unwrap();
    id_map
}

fn main() {
    let opt = Opt::from_args();

    let revs_to_paths = get_revs_for_paths(opt.paths);
    // println!("{:?}", revs);
    let mut paths_to_count: HashMap<PathBuf, u32> = HashMap::new();
    revs_to_paths.iter().for_each(|(_rev, path)| {
        let previous = paths_to_count.insert(path.clone(), 1);
        if let Some(count) = previous {
            paths_to_count.insert(path.clone(), count + 1);
        }
    });
    println!("{:#?}", paths_to_count);


    let sizes = get_sizes_of_objects(revs_to_paths.keys().collect());

    // This skips directories (they have no size mapping).
    // Filename -> size mapping tuples. Files are present in the list more than once.
    let file_sizes: Vec<(&Path, u64)> = sizes
        .iter()
        .map(|(id, size)| (revs_to_paths[id].as_path(), *size))
        .collect();

    // (Filename, size) tuples.
    let mut file_size_sums: HashMap<&Path, u64> = HashMap::new();
    for (mut path, size) in file_sizes.into_iter() {
        if opt.directories {
            // For file path "foo/bar", add these bytes to path "foo/"
            let parent = path.parent();
            path = match parent {
                Some(parent) => parent,
                _ => {
                    eprint!("File has no parent directory: {}", path.display());
                    continue;
                }
            };
        }

        *(file_size_sums.entry(path).or_default()) += size;
    }
    let sizes: Vec<(&Path, u64)> = file_size_sums.into_iter().collect();

    print_sizes(sizes);
}

fn print_sizes(mut sizes: Vec<(&Path, u64)>) {
    sizes.sort_by_key(|(_path, size)| *size);
    for file_size in sizes.iter() {
        // The size needs some padding--a long size is as long as a tabstop
        println!("{:10}{}", ByteSize(file_size.1), file_size.0.display())
    }
}