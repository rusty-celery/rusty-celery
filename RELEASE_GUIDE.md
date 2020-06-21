# Publishing a new release

1. Set the environment variable `VERSION` to the desired version and change the version in all subcrate `Cargo.toml` files and in the root `Cargo.toml`. Also change the version of the subcrate dependencies in the root `Cargo.toml`. All versions should now match `VERSION`.

2. Update the [CHANGELOG]("CHANGELOG.md").

3. Commit and push these changes:

    ```bash
    git commit -a -m "(cargo-release) version $VERSION"
    git push
    ```

4. Add a tag in git to mark the release:

    ```bash
    git tag "v$VERSION" -m "Adds tag v$VERSION for cargo"
    git push --tags origin master
    ```

5. Lastly, go to the corresponding release on GitHub and copy over the notes from the CHANGELOG. Then add a "Commits" section with the output of this command:

    ```bash
    git log `git describe --always --tags --abbrev=0 HEAD^^`..HEAD^ --oneline
    ```

    Or, if you're using fish shell:

    ```fish
    git log (git describe --always --tags --abbrev=0 HEAD^^)..HEAD^ --oneline
    ```

    The GitHub Actions release workflow will handle testing and publishing to crates.io.

# Fixing a failed release

If for some reason the GitHub Actions release workflow failed with an error that needs to be fixed, you'll have to delete both the tag and corresponding release from GitHub. After you've pushed a fix, delete the tag from your local clone with

```bash
git tag -l | xargs git tag -d && git fetch -t
```

Then repeat the steps above.
