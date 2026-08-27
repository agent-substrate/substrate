#include <stdio.h>
#include <string.h>

#include "czmq.h"

void startExternalRouter(int port)
{
    zsock_t *router = zsock_new (ZMQ_ROUTER);
    assert (router);
    int rc = zsock_bind (router, "tcp://127.0.0.1:%d", port);
	fprintf(stderr, "rc: [%d]\n", rc);

	char *ident = NULL;
    zstr_recvx(router, &ident, NULL);

	fprintf(stderr, "received id: [%s]\n", ident);

    zstr_sendm (router, ident);
    zstr_send (router, "WORLD");

    zsock_destroy (&router);
}
