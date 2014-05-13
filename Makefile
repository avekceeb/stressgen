all: clean stressgen

clean:
	rm -f *.o stressgen

stressgen: stressgen.c
	gcc -DSYSLOGGING -pthread -O2 -Wall stressgen.c -o stressgen

# SunOS) OPTS="-lsocket -lnsl -lpthread -O2 -Wall" ;; \

