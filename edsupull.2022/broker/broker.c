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



typedef struct client
{
    UUID_t uuid;        // UUID
    int sd;             // socket descriptor ?
    set *topics;         //topics descriptors to which the client is subcribed
    queue *events;     //envents queued so the client can pull them in order
} client;

typedef struct topic
{
    const char *nombre;  //topic's key
    set *clients;       //descriptors of the subscribed clients
} topic;

typedef struct event
{
    const char *topic;   //event's key
    const void *value;  //value of the event
    uint32_t tam_evento;
    int queue;          //number of clients queues awaiting for get op
    
}event;


map *clients_map;
map *topics_map;

// create clients struct and inserts it into the map
client * create_client(map *mg, UUID_t uuidI, int sd)
{
    client *c = malloc(sizeof(client));
    strcpy(c->uuid,uuidI);
    c->topics = set_create(0);
    c->events = queue_create(0);
    map_put(mg, c->uuid, c);
    return c;
}

topic * create_topic(map *mg, const char *nombre)
{
    topic * t = malloc(sizeof(topic));
    t->nombre = nombre;
    t->clients = set_create(0);
    map_put(mg,t->nombre,t);
    return t;
}

event * create_event(const char *topic, const void *value, uint32_t tam)
{
    event * e = malloc(sizeof(event));
    e->topic = topic;
    e->value = value;
    e->tam_evento = tam;
    e->queue = 0;
    return e;
}

void decreaseCount(void *a)
{
    event * e = a;
    e->queue--;
    if(e->queue == 0)
    {
        free(e);
    }
}

int destroyClient(client *c)
{
    int res = 0;
    set_iter *it = set_iter_init(c->topics);
    while(set_iter_has_next(it))
    {
        topic *t = set_iter_value(it);
        set_remove(t->clients,c,NULL);
        set_iter_next(it);
    }
    res = map_remove_entry(clients_map,c->uuid, NULL);
    res =set_destroy(c->topics, NULL);
    res =queue_destroy(c->events,decreaseCount);
    return res;
    
}





// register a new client into the client's map
void registerClient(int s_srv)
{
    UUID_t uuid;
    
    if(recv(s_srv, uuid, sizeof(UUID_t), MSG_WAITALL) < 0)
    {
        perror("Error al recibir el uuid del client\n");
    }
    create_client(clients_map, uuid, s_srv);
    int res = 0;
    if(send(s_srv, &res, sizeof(int), 0) < 0)
    {
        perror("Error al enviar la respuesta al client\n");
    }
}

// subscribe client if exist to the given topic if also exist
void subscribeToTopic(int s_srv)
{
    UUID_t uuid;
    int len;
    char *topicId;

    if(recv(s_srv, uuid, sizeof(UUID_t), MSG_WAITALL) < 0)
    {
        perror("Error al recibir el uuid del client\n");
    }

    if(recv(s_srv, &len, sizeof(int), MSG_WAITALL) < 0 )
    {
        perror("Error al recibir la longitud del topic\n");
    }

    len = ntohl(len);
    topicId = malloc(len+1);

    if(recv(s_srv, topicId, len, MSG_WAITALL) < 0)
    {
        perror("Error al recibir el uuid del client\n");
    }

    topicId[len]='\0';
    int res = 0;
    int getC;
    int getT;
    client * c;
    topic * t;

    c = map_get(clients_map, uuid, &getC);
    t = map_get(topics_map, topicId, &getT);

    
    if(getC == -1 || getT == -1 || set_add(c->topics, t) == -1 || set_add(t->clients, c) == -1)
    {
        res = -1; 
    }
    

    if(send(s_srv, &res, sizeof(int), 0) < 0)
    {
        perror("Error al enviar la respuesta al client\n");
    }
}

//publish an event and queue it to the subscibed clients
void publish(int s_srv)
{
    
    struct cabecera cab;
    char *topicId;
    void *eventId;

    if(recv(s_srv, &cab, sizeof(cab), MSG_WAITALL) < 0 )
    {
        perror("Error al recibir la cabecera\n");
    }

    int tam1 = ntohl(cab.long1);
    int tam2 = ntohl(cab.long2);
    topicId = malloc(tam1+1);
    eventId = malloc(tam2+1);

    if(recv(s_srv, topicId, tam1, MSG_WAITALL) < 0)
    {
        perror("Error al recibir el topic\n");
    }
    if(recv(s_srv, eventId, tam2, MSG_WAITALL) < 0)
    {
        perror("Error al recibir el event\n");
    }
    topicId[tam1] = '\0';
    int getT;
    int res = 0;
    topic *t = map_get(topics_map, topicId, &getT);
    event *e = create_event(topicId,eventId, tam2);

    if(getT == 0)
    {
        set_iter *it = set_iter_init(t->clients);
        while(set_iter_has_next(it))
        {
            client *c = set_iter_value(it);
            queue_push_back(c->events, e);
            e->queue++;
            set_iter_next(it);
        }
    }
    else
    {
        res = -1;
    }
    
    if(send(s_srv, &res, sizeof(int), 0) < 0)
    {
        perror("Error al enviar la respuesta al client\n");
    }
}

// send the first event in the client's queue
void get(int s_srv)
{
    UUID_t uuid;
    
    if(recv(s_srv, uuid, sizeof(UUID_t), MSG_WAITALL) < 0)
    {
        perror("Error al recibir el uuid del client\n");
    }
    int getC;
    int getE;
    client *c = map_get(clients_map, uuid, &getC);
    event *e = queue_pop_front(c->events,&getE);
    if(getC != -1 && getE != -1)
    {
        e->queue--;
        struct cabecera cab;
        cab.long1 = htonl(strlen(e->topic));
        cab.long2 = htonl(e->tam_evento);
        struct iovec iov[3];
        iov[0].iov_base=&cab;
        iov[0].iov_len=sizeof(cab);
        iov[1].iov_base=e->topic;
        iov[1].iov_len=strlen(e->topic);
        iov[2].iov_base=e->value;
        iov[2].iov_len=e->tam_evento;
        if (writev(s_srv, iov, 3)<0)
        {
            perror("error al escribir los datos para la subscripcion");
        }
        if(e->queue == 0)
        {
            free(e);
        }
    }
    
}

//unsubscribes the client from the incoming topic

void unsubscribeFromTopic(int s_srv)
{
    UUID_t uuid;
    int len;
    char *topicId;

    if(recv(s_srv, uuid, sizeof(UUID_t), MSG_WAITALL) < 0)
    {
        perror("Error al recibir el uuid del client\n");
    }

    if(recv(s_srv, &len, sizeof(int), MSG_WAITALL) < 0 )
    {
        perror("Error al recibir la longitud del topic\n");
    }

    len = ntohl(len);
    topicId = malloc(len+1);

    if(recv(s_srv, topicId, len, MSG_WAITALL) < 0)
    {
        perror("Error al recibir el uuid del client\n");
    }

    topicId[len]='\0';
    int res = 0;
    int getC;
    int getT;
    client * c;
    topic * t;

    c = map_get(clients_map, uuid, &getC);
    t = map_get(topics_map, topicId, &getT);

    
    if(getC == -1 || getT == -1 || set_remove(c->topics, t, NULL) == -1 || set_remove(t->clients, c, NULL) == -1)
    {
        res = -1; 
    }

    if(send(s_srv, &res, sizeof(int), 0) < 0)
    {
        perror("Error al enviar la respuesta al client\n");
    }
}

//deregisters client from broker and erases it descriptor
void closeClient(int s_srv)
{
    UUID_t uuid;
    
    if(recv(s_srv, uuid, sizeof(UUID_t), MSG_WAITALL) < 0)
    {
        perror("Error al recibir el uuid del client\n");
    }
    int res = 0;
    int getC;
    client * c;

    c = map_get(clients_map, uuid, &getC);
    if(getC != -1)
    {
       res = destroyClient(c);
       if(res = 0)
       {
           free(c);
       }
    }
    else{
        res = -1;
    }

    if(send(s_srv, &res, sizeof(int), 0) < 0)
    {
        perror("Error al enviar la respuesta al client\n");
    }
    close(s_srv);

}

// send the number of clients registered in the broker
void clients(int s_srv)
{
    int tam = map_size(clients_map);
    if(send(s_srv, &tam, sizeof(int), 0) < 0)
    {
        perror("Error al enviar el número de clientes\n");
    }
}

// send the number of topics stored in the broker
void topics(int s_srv)
{
    int tam = map_size(topics_map);
    if(send(s_srv, &tam, sizeof(int), 0) < 0)
    {
        perror("Error al enviar el número de topics\n");
    }
}

// ssend the number of clients subscribed to given topic
void subscibers(int s_srv)
{
    int len;
    char *topicId;
    
    
    if(recv(s_srv, &len, sizeof(int), MSG_WAITALL) < 0 )
    {
        perror("Error al recibir la longitud del topic\n");
    }

    len = ntohl(len);
    topicId = malloc(len+1);

    if(recv(s_srv, topicId, len, MSG_WAITALL) < 0)
    {
        perror("Error al recibir el uuid del client\n");
    }

    topicId[len]='\0';
    int res = 0;
    int getT;
    topic * t;

    t = map_get(topics_map, topicId, &getT);
    if( getT == -1)
    {
        res = -1; 
    }
    else
    {
        res=set_size(t->clients);
    }
    
    if(send(s_srv, &res, sizeof(int), 0) < 0)
    {
        perror("Error al enviar la respuesta al client\n");
    }

}

// send the number of events waiting for pull
void events(int s_srv)
{
    UUID_t uuid;

    if(recv(s_srv, uuid, sizeof(UUID_t), MSG_WAITALL) < 0)
    {
        perror("Error al recibir el uuid del client\n");
    }

    int getC;
    client *c;
    int res;

    c = map_get(clients_map, uuid, &getC);
    if(getC == -1)
    {
        res = -1;
    }
    else
    {
        res = queue_length(c->events);   
    }
    if(send(s_srv, &res, sizeof(int), 0) < 0)
    {
        perror("Error al enviar el número de clientes\n");
    }
}



// gives service to the client
void *service(void *arg)
{
    int s_srv = (long) arg;
    int op;
    
    while(recv(s_srv, &op, sizeof(int), MSG_WAITALL) > 0)
    {
        switch (op)
        {
        case REGISTERCLIENT:
            registerClient(s_srv);
            break;
        case CLIENTS:
            clients(s_srv);
            break;
        case TOPICS:
            topics(s_srv);
            break;
        case SUBSCRIBE:
            subscribeToTopic(s_srv);
            break;
        case SUBSCRIBERS:
            subscibers(s_srv);
            break;
        case PUBLISH:
            publish(s_srv);
            break;
        case EVENTS:
            events(s_srv);
            break;
        case GET:
            get(s_srv);
            break;
        case UNSUBSCRIBE:
            unsubscribeFromTopic(s_srv);
            break;
        case CLOSECLIENT:
            closeClient(s_srv);
        default:
            break;
        }
    }
    
	return NULL;
}

// read topics from ftopics
void readTopicsfile(char *file)
{
    FILE *s;
    char *topic;
    s = fopen(file,"r");
    while (!feof(s))
    {
        fscanf(s,"%ms", &topic);
        create_topic(topics_map,topic);
    }
    fclose(s);
}

int main(int argc, char *argv[])
{
    int s, s_conec;
    unsigned int tam_dir;
    struct sockaddr_in dir_cliente;
    int opcion=1;
    clients_map = map_create(key_string,0);
    topics_map = map_create(key_string, 0);
    
    if(argc!=3)
    {
        fprintf(stderr, "Uso: %s puerto fichero_topics\n", argv[0]);
        return 1;
    }

    readTopicsfile(argv[2]);

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
	    pthread_create(&thid, &atrib_th, service, (void *)(long)s_conec);
    }
    //when finished close the socket
    close(s);
    return 0;
}


