# Hacking Rusty Celery

If you're new to Rust this will show you how to set up your development environment for Rusty Celery and start making contributions. After going through this guide, search for issues with the label ["Good first issue"](https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aissue+is%3Aopen+label%3A%22Good+first+issue%22) to find a place to start.

## Installing the Rust toolchain

The only big thing you'll need to manually install is the Rust toolchain. Luckily this is actually pretty painless and can be accomplished with a single line of code:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

For more information see the official [install guide](https://www.rust-lang.org/tools/install).

## Forking your own copy of Rusty Celery

If you're already familiar with git and GitHub you can skip this section. If not, this will show you how to create local copy of the repository that you can easily keep clean and up-to-date with the main repo.

The first step is to fork the Rusty Celery on GitHub. Once you've done that you can clone your fork locally with either

```bash
git clone https://github.com/USERNAME/rusty-celery.git
```

or 

```bash
git clone git@github.com:USERNAME/rusty-celery.git
```

if you want to clone using SSH.

The next step is to add a new remote URL called `upstream` that points to the main repo. To do this, just copy and paste this command into your terminal:

```bash
git remote add upstream https://github.com/rusty-celery/rusty-celery.git
```

To check that this worked you can try `git remote -v` and you should see a remote called `origin` which points to your fork on GitHub and the new `upstream` remote which points to main repo.

Now every time you want to pull changes from the main repo you can do this:

```bash
git checkout main  # if not already on main
git pull --rebase upstream main
git push
```

If you visit your fork of the project on GitHub it should say that your main branch is up-to-date with [rusty-celery/rusty-celery](https://github.com/rusty-celery/rusty-celery). Then when you're preparing to make a contribution you can create a new branch to work on your fix/improvement/feature:

```bash
git checkout -b BRANCH
git push --set-upstream origin BRANCH
```

## Build, test, lint, and run examples

One of things that makes the Rust toolchain so awesome is it's simplicity. Everything you need to build, test, lint, and run code in this project is provided by the `cargo` command. To start, try the following:

```bash
# Compile a dev version.
cargo build

# Run unit tests and doc tests.
cargo test --lib
cargo test --doc

# Build a local copy of the API documentation.
cargo doc --open
```

There are two additional `cargo` "plugins" that we use: [rustfmt](https://github.com/rust-lang/rustfmt) for automatically formatting code and [Clippy](https://github.com/rust-lang/rust-clippy) for catching common mistakes. You can quickly install these to your toolchain with

```bash
rustup component add rustfmt
rustup component add clippy
```

Then run them with

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

> Clippy may fail with several error messages which is okay. We actually tell Clippy to ignore those in our CI.

Since some of these commands can get a little verbose and you'll be using them so often, the [`Makefile`](https://github.com/rusty-celery/rusty-celery/blob/main/Makefile) actually has shortcuts for running all of these and then some. For instance, running `make build`, `make test`, `make doc`, and `make lint` wraps the above commands, respectively.

> Including a Makefile in a Rust project is definitely not a standard thing to do but it's a nice little productivity hack.

## Project structure

Now that you know how to build and run tests you can start peaking into the source code.

Rusty Celery is organized like a typical Rust project:

- [`Cargo.toml`](https://github.com/rusty-celery/rusty-celery/blob/main/Cargo.toml) defines the what the crate (what Rust folk call a library) is called (`celery` in this case) and other meta data, along with all of the dependencies it requires. You'll also notice at the top of the file is a section called `[workspace]`. This is there because the project technically contains more than one crate. The subcrates are not meant to be standalone libraries but are of course closely tied with the root crate. If you look in one of the subcrates you'll also find a `Cargo.toml`.
- [`src/`](https://github.com/rusty-celery/rusty-celery/tree/main/src) is where the source code of the main crate is located and is where you'll most likely be making changes.
- [`src/lib.rs`](https://github.com/rusty-celery/rusty-celery/blob/main/src/lib.rs) is what's called the "crate root" and defines which submodules are visible publicly and internally. For example, the line that says `mod app;` makes the `app` module visibly internally so that code from other modules can use things in the app module with `crate::app::*`. On the hand, where you see something like `pub use app::Celery` means the struct `Celery` will be publicly available as `celery::Celery`.
- [`examples/`](https://github.com/rusty-celery/rusty-celery/tree/main/examples) contains runnable examples. Any Rust files in this directory will have a link to them in the `Cargo.toml` file which means you can run them with `cargo run`. For instance: `cargo run --example celery_app`.
- [`tests/`](https://github.com/rusty-celery/rusty-celery/tree/main/tests) contains integration tests. The are meant to test typical usage of the public API. Unit tests on the other hand are actually located within `src/` files (usually at the bottom) where you see a `#[cfg(test)]` macro. This macro means that code will only be compiled when running tests.

## Rust basics

Although a lot of this project relies on some more advanced features of the Rust like [async / await](https://rust-lang.github.io/async-book/01_getting_started/01_chapter.html) and [macros](https://doc.rust-lang.org/book/ch19-06-macros.html), most of the language is very intuitive and easy to read for a beginner. You should be able to get started tinkering and fixing small bugs by just knowing a few of the basics:

- [Match](https://doc.rust-lang.org/stable/rust-by-example/flow_control/match.html) and [Patterns](https://doc.rust-lang.org/book/ch18-00-patterns.html)
- [Options](https://doc.rust-lang.org/stable/rust-by-example/std/option.html)
- [Expression](https://doc.rust-lang.org/stable/rust-by-example/expression.html)
- [Traits](https://doc.rust-lang.org/stable/rust-by-example/trait.html)

For a more comprehensive resource see [**The Book**](https://doc.rust-lang.org/book/title-page.html).
