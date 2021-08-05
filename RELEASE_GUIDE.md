# Publishing a new release

1. Set the environment variable `TAG`, which should be of the form `v{VERSION}`, where `VERSION` is the target version of the release.

2. Change version in all subcrate `Cargo.toml` files and in the root `Cargo.toml` to the target version. Also change the version of the subcrate dependencies in the root `Cargo.toml`. All versions should now match the target version.

3. Update the [CHANGELOG]("CHANGELOG.md") by creating a new release section header right the `## Unreleased` header.

4. Commit and push these changes:

    ```bash
    git commit -a -m "(cargo-release) $TAG" && git push
    ```

5. Add a tag in git to mark the release:

    ```bash
    git tag "$TAG" -m "Adds tag $TAG for release" && git push --tags
    ```

# Fixing a failed release

If for some reason the GitHub Actions release workflow failed with an error that needs to be fixed, you'll have to delete both the tag and corresponding release from GitHub. After you've pushed a fix, delete the tag from your local clone with

```bash
git tag -l | xargs git tag -d && git fetch -t
```

Then repeat the steps above.
