# Contributing to DTPatternRecognition

:+1: First off, thanks for reading this contributing guide! :+1:

Here are some guidelines we'd like you to follow:

#### Table Of Contents
* [Questions and Problems](#question)
* [Issues and Bugs](#issue)
* [Feature Requests](#feature)
* [Improving Documentation](#docs)
* [Issue Submission Guidelines](#submit)
* [Pull Requests and Submission Guidelines](#submit-pr)

## <a name="requests"></a> Questions, Bugs, Features

### <a name="question"></a> Got a Question or Problem?

Open an issue with label https://github.com/INTREPID-hep/DTPatternRecognition/labels/question

### <a name="issue"></a> Found an Issue or Bug?

If you find a bug in the source code, you can submit a Bug Report to our
[Repository][github-issues]. Even better, you can submit a Merge Request with a fix.

**Please see the [Submission Guidelines](#submit) below.**

### <a name="feature"></a> Missing a Feature?

You can request a new feature by submitting an issue to the [GitHub Repository][github-issues]. If you would like to implement it having into account the [developer guidelines][developers].

### <a name="docs"></a> Want a Doc Fix?

Should you have a suggestion for the documentation, you can open an issue and outline the problem
or improvement you have - however, creating the doc fix yourself is much better!

If you want to improve anything it's a good idea to let others know what you're working on to
minimize duplication of effort. Always try to Create a new issue (or comment on a related existing one) to let
others know what you're working on. Try to write commit message following the standar Commit Message Guidelines.

## <a name="submit"></a> Issue Submission Guidelines
Before you submit your issue search the archive, maybe your question was already answered.

If your issue appears to be a bug, and hasn't been reported, open a new issue.

In the "[new issue][github-new-issue]" please add one of the following labels:
https://github.com/INTREPID-hep/DTPatternRecognition/labels/bug/https://github.com/INTREPID-hep/DTPatternRecognition/labels/enhancement/https://github.com/INTREPID-hep/DTPatternRecognition/labels/question/https://github.com/INTREPID-hep/DTPatternRecognition/labels/documentation.

## <a name="submit-pr"></a> Pull Requests and Submission Guidelines
Before you submit your work consider the following guidelines:

* Take a look to [development guidlines][developers] to know how to setup your develoment workspace.
* Make your changes in a new git branch:

    ```shell
    git checkout -b my-dev-branch
    ```
    **Important**:
    - You do not need to fork the repository, instead you can create a new branch in the central repository.
    - The branch name should start with a meaningful name such as `(feature|bugfix|cleanup)-*`.

* Test your code.

    - You can run the tests locally using [pytest](https://docs.pytest.org/):
      ```shell
      poetry run pytest
      ```
      or, if you are not using Poetry:
      ```shell
      pytest
      ```
    - All tests (including code style checks) will also be run automatically when you open a pull request to the `main` branch, thanks to our GitHub Actions CI pipeline.

* Create your patch commit.
    - If it's your first commit in this repository, add yourself to the `CONTRIBUTORS` file

* Change or add relevant documentation.

* Commit your changes using a descriptive commit message
    ```shell
    git add <list of files you have modified>
    git commit 
    ```
  Note: do not add to your commit binary files, libraries, build artefacts etc.

* Push your branch to GitHub:

    ```shell
    git push origin my-dev-branch
    ```
* In GitHub, open a merge request to `DTPatternRecognition:main`. 

* If we suggest changes, then:

  * Make the required updates.
  * Re-run all the applicable tests
  * Commit your changes to your branch (e.g. `my-dev-branch`).
  * Push the updated branch to the GitHub repository (this will update your Merge Request).

    You can also amend the initial commits and force push them to the branch.

    ```shell
    git rebase develop -i
    git push -f origin my-dev-branch
    ```

    This is generally easier to follow, but separate commits are useful if the Merge Request contains
    iterations that might be interesting to see side-by-side.

That's it! Thank you for your contribution!

#### After your pull request is merged

The branch you have created will be automatically deleted from the central repository.

* Check out the main branch:

    ```shell
    git checkout main -f
    ```

* Delete the local branch:

    ```shell
    git branch -D my-dev-branch
    ```

* Update your main with the latest upstream version:

    ```shell
    git pull --ff origin main
    ```

[github]: https://github.com/INTREPID-hep/DTPatternRecognition
[github-issues]: https://github.com/INTREPID-hep/DTPatternRecognition/issues
[github-new-issue]:https://github.com/INTREPID-hep/DTPatternRecognition/issues/new
[developers]:DEVELOPERS.md
