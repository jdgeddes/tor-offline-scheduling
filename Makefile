all:
		gcc -g -Wall -std=c99 `pkg-config --cflags glib-2.0` tor-offline-scheduling.c -o tor-offline-scheduling `pkg-config --libs glib-2.0`

clean:
		rm *.o tor-offline-scheduling
