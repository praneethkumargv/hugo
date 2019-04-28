GODEBUG=http2debug=1   # enable verbose HTTP/2 debug logs
GODEBUG=http2debug=2   # ... even more verbose, with frame dumps

go install napoleon
napoleon -cport 8079 -rpc 1 -name=s1 -ip=localhost