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
    UUID_t* uuid;        // UUID
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
cliente * crea_cliente(map *mg, UUID_t* uuid, int sd)
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


map *mapa_clientes;
map *mapa_temas;

// register a new client into the client's map
void registerClient(int s_srv)
{
    UUID_t uuid;
    recv(s_srv, uuid, sizeof(UUID_t), MSG_WAITALL);
    crea_cliente(mapa_clientes, &uuid, s_srv);
    printf("El cliente se ha registrado correctamente\n");
}

// send the number of clients registered in the broker
void clients(int s_srv)
{
    int tam = map_size(mapa_clientes);
    if(send(s_srv, &tam, sizeof(int), 0) < 0)
    {
        perror("Error al enviar el número de clientes");
    }
}

void *servicio(void *arg)
{
    int s_srv = (long) arg;
    int op;
    printf("Thread para el servicio creado\n");
    

    while(recv(s_srv, &op, sizeof(int), MSG_WAITALL) > 0)
    {
        switch (op)
        {
        case REGISTERCLIENT:
            registerClient(s_srv);
            break;
        case CLIENTS:
            clients(s_srv);
        default:
            break;
        }
    }
    
	return NULL;
}

int main(int argc, char *argv[])
{
    int s, s_conec;
    unsigned int tam_dir;
    struct sockaddr_in dir_cliente;
    int opcion=1;
    mapa_clientes = map_create(key_int,0);
    mapa_temas = map_create(key_string, 0);
    
    printf("inicio\n");
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
    printf("bind\n");
    if (bind(s, (struct sockaddr *)&dir, sizeof(dir)) < 0)
    {
        perror("error en bind");
        close(s);
        return 1;
    }

    //listen for connections
    printf("listen\n");
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
        printf("Esperando conexion\n");
        if ((s_conec=accept(s, (struct sockaddr *)&dir_cliente, &tam_dir))<0)
        {
            perror("Error en accept");
            close(s);
            return 1;
        }
        printf("Conexion recibida\n");
	    pthread_create(&thid, &atrib_th, servicio, (void *)(long)s_conec);
    }
    //when finished close the socket
    close(s);
    return 0;
}


