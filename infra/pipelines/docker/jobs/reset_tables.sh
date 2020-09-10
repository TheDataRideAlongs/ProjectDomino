#!/bin/bash
# to run iptables commands you need to be root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root."
    return 1
fi



echo 'flushing... '
iptables -P INPUT ACCEPT
iptables -P FORWARD ACCEPT
iptables -P OUTPUT ACCEPT
iptables -t nat -F
iptables -t mangle -F
iptables -F
iptables -X

echo 'current rules are: '

iptables -S