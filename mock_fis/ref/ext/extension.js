const vscode = require('vscode');

const TERMINALS = [
    { name: 'fis',         color: new vscode.ThemeColor('terminal.ansiYellow')   },
    { name: 'interrogate', color: new vscode.ThemeColor('terminal.ansiGreen')  },
    { name: 'cleanup',     color: new vscode.ThemeColor('terminal.ansiBlue') },
];

function openTerminals() {
    for (const opts of TERMINALS) {
        const exists = vscode.window.terminals.some(t => t.name === opts.name);
        if (!exists) {
            vscode.window.createTerminal(opts);
        }
    }
}

function activate(context) {
    context.subscriptions.push(
        vscode.commands.registerCommand('mock-fis-terminals.open', openTerminals)
    );
    openTerminals();
}

function deactivate() {}

module.exports = { activate, deactivate };
