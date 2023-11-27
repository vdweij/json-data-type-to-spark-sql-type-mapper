# Development information

## VSCode

### Debugging

In order to be able to leverage debugging unit tests, the following configuration is needed to be added to `launch.json`:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Python: Unittest",
      "type": "python",
      "request": "launch",
      "program": "${file}",
      "purpose": ["debug-test"],
      "console": "integratedTerminal",
      "env": {"PYTHONPATH": "${workspaceFolder}${pathSeparator}${env:PYTHONPATH}"}
    }
  ]
}
```

This file can be made manually or through the Run & Debug menu option and then clicking on the gear for options.

See also a StackOverflow's issue: [how to debug python unittests in Visual Studio Code?](https://stackoverflow.com/questions/72992588/how-to-debug-python-unittests-in-visual-studio-code/77556449#77556449)