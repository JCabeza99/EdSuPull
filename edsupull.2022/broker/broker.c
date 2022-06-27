#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "util/set.h"
#include "util/map.h"
#include "util/queue.h"
#include "comun.h"


typedef struct cliente
{
    const char *uuid;     // UUID
    int sd;             // socket descriptor ?
    set *temas;         //topics descriptors to which the client is subcribed
    queue *eventos;     //envents queued so the client can pull them in order
} cliente;

typedef struct tema
{
    const char *nombre;  //topic's key
    set *clientes;       //descriptors of the subscribed clients
} tema;

typedef struct evento
{
    const char *tema;   //event's key
    int value       //value in binary
}evento;


// create clients struct and inserts it into the map
cliente * crea_cliente(map *mg, const char *uuid, int sd)
{
    cliente *c = malloc(sizeof(cliente));
    c->uuid = uuid;
    c->sd = sd;
    c->temas = set_create(0);
    c->eventos = queue_create(0);
    map_put(mg, c->uuid, c);
    return c;
}

tema * crea_tema(map *mg, const char *nombre)
{
    tema * t = malloc(sizeof(tema));
    t->nombre = nombre;
    t->clientes = set_create(0);
    map_put(mg,t->nombre,t);
    return t;
}

evento * crea_evento(const char *tema, int value)
{
    evento * e = malloc(sizeof(evento));
    e->tema = tema;
    e->value = value;
    return e;
}


void *servicio(void *arg)
{
        int s_srv, tam;
        int op_tamC;
        s_srv=(long) arg;
        printf("nuevo cliente\n");
        while (recv(s_srv, &tam, sizeof(tam), MSG_WAITALL)>0)
        {
            printf("recibida petición cliente\n");
            //guardo tamaño
            int tamn=ntohl(tam);
            //reservo espacio en memmoria
            char *dato = malloc(tamn);
            //recibo datos
            recv(s_srv, dato, tamn, MSG_WAITALL);
            //trabajo con ellos
            //los envio
            send(s_srv, dato, tamn, 0);
        }
        close(s_srv);
	return NULL;
}

int main(int argc, char *argv[])
{
    int s, s_conec;
    unsigned int tam_dir;
    struct sockaddr_in dir_cliente;
    int opcion=1;
    map *mapa_clientes = map_create(key_int,0);
    map *mapa_temas = map_create(key_string, 0);
    

    if(argc!=3)
    {
        fprintf(stderr, "Uso: %s puerto fichero_temas\n", argv[0]);
        return 1;
    }

    //server socket creation
    if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
    {
        perror("error creando socket");
        return 1;
    }
    
    //to set socket options
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opcion, sizeof(opcion))<0)
    {
        perror("error en setsockopt");
        return 1;
    }

    //server address 
    struct sockaddr_in dir;
    dir.sin_family= AF_INET;
    dir.sin_addr.s_addr=INADDR_ANY;
    dir.sin_port=htons(atoi(argv[1]));
    

    //bind socket to address
    if (bind(s, (struct sockaddr *)&dir, sizeof(dir)) < 0)
    {
        perror("error en bind");
        close(s);
        return 1;
    }

    //listen for connections
    if (listen(s, 5) < 0)
    {
        perror("error en listen");
        close(s);
        return 1;
    }

    //thread declaration atributes
    pthread_t thid;
    pthread_attr_t atrib_th;
    pthread_attr_init(&atrib_th);
    pthread_attr_setdetachstate(&atrib_th, PTHREAD_CREATE_DETACHED);
    while(1)
    {
        tam_dir=sizeof(dir_cliente);
        //This is where we accept connections and save the descriptor for the new socket created
        //to send and receive data
        if ((s_conec=accept(s, (struct sockaddr *)&dir_cliente, &tam_dir))<0)
        {
            perror("Error en accept");
            close(s);
            return 1;
        }

        struct cabecera cab;
        recv (s_conec, &cab, sizeof(cab), MSG_WAITALL);
        int tam1=ntohl(cab.long1);
		int tam2=ntohl(cab.long2);
        UUID_t* uuid = malloc(tam1+1);
        int op = malloc(tam2+1);
        recv(s_conec, uuid, tam1, MSG_WAITALL);
		recv(s_conec, op, tam2, MSG_WAITALL);
        printf("uuid: %s op: %d\n", uuid, op);
        
        
        //if(recv(s_conec, op_id, sizeof(op_id),  MSG_WAITALL)>0)
	    //pthread_create(&thid, &atrib_th, servicio, (void *)(long)s_conec);
    }
    //when finished close the socket
    close(s);
    return 0;
}


