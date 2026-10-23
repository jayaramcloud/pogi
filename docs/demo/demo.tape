# Records docs/images/demo.gif with charmbracelet/vhs. Run from the repo root, with nothing on port 7644:
#   vhs docs/demo/demo.tape
Output docs/images/demo.gif
Set Shell zsh
Set FontSize 16
Set Width 1100
Set Height 600
Set Padding 24
Set Theme "Catppuccin Mocha"
Set TypingSpeed 90ms
Hide
Type "export POGI_DATA=$(mktemp -d); pogi(){ command pogi start --data-dir $POGI_DATA; }; ( sh docs/demo/traffic.sh & ); clear"
Enter
Show
Sleep 400ms
Type "pogi"
Sleep 400ms
Enter
Sleep 9s
Type "2"
Sleep 4s
Type "7"
Sleep 5s
