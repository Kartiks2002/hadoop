# hadoop
Where big data meets even bigger confusion

Color	Code	Example
Black	30	\e[30m
Red	31	\e[31m
Green	32	\e[32m
Yellow	33	\e[33m
Blue	34	\e[34m
Magenta	35	\e[35m
Cyan	36	\e[36m
White	37	\e[37m
Default	39	\e[39m

vim ~/.bashrc
source ~/.bashrc

PS1='\[\e[1;36m\]\u@\h \[\e[1;34m\]\w \[\e[1;32m\]$ \[\e[0m\]' # username : cyan, $ : red 
