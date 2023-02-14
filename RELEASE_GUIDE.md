# Publishing a new release

1. Install `toml-cli` if you haven't already (just run `cargo install toml-cli`).

2. Change the version in all sub-crate `Cargo.toml` files and in the root `Cargo.toml` to the target version. Also change the version of the sub-crate dependencies in the root `Cargo.toml`. All versions should now match the target version.

3. Run the script `./scripts/release.sh` and follow the prompts.

# Fixing a failed release

If for some reason the GitHub Actions release workflow failed with an error that needs to be fixed, you'll have to delete both the tag and corresponding release from GitHub. After you've pushed a fix, delete the tag from your local clone with

```bash
git tag -l | xargs git tag -d && git fetch -t
```

Then repeat the steps above.
