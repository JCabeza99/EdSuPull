#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>

#include "edsu.h"
#include "comun.h"


int s;
UUID_t uuid;

struct sockaddr_in dir;
struct hostent *host_info;

// se ejecuta antes que el main de la aplicación
__attribute__((constructor)) void inicio(void){
    if (begin_clnt()<0) {
        fprintf(stderr, "Error al iniciarse aplicación\n");
        // terminamos con error la aplicación antes de que se inicie
	// en el resto de la biblioteca solo usaremos return
        _exit(1);
    }
}

// se ejecuta después del exit de la aplicación
__attribute__((destructor)) void fin(void){
    if (end_clnt()<0) {
        fprintf(stderr, "Error al terminar la aplicación\n");
        // terminamos con error la aplicación
	// en el resto de la biblioteca solo usaremos return
        _exit(1);
    }
}


// operaciones que implementan la funcionalidad del proyecto
int begin_clnt(void){
    if(generate_UUID(uuid)< 0)
    {
        perror("Error generando el uuid");
        return -1;
    }

    //taking host info to make the connection
    host_info=gethostbyname(getenv("BROKER_HOST"));
	memcpy(&dir.sin_addr.s_addr, host_info->h_addr_list[0], host_info->h_length);
	dir.sin_port=htons(atoi(getenv("BROKER_PORT")));
	dir.sin_family=AF_INET;
    
    //socket creation

    if((s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
    {
        perror("Error creando socket");
        return -1;
    }

    //connection establishment

    if(connect(s,(struct sockaddr *)&dir, sizeof(dir)) < 0)
    {
        perror("Error al hacer la conexion");
        return -1;
    }
    
    //sending of operation code and uuid to register client into the system
    int op = REGISTERCLIENT;
    struct iovec iov[2];
    iov[0].iov_base=&op;
    iov[0].iov_len=sizeof(int);
    iov[1].iov_base=&uuid;
    iov[1].iov_len=sizeof(UUID_t);
    
    if(writev(s,iov, 2) < 0)
    {
        perror("Error en writev");
        close(s);
        return 1;
    }
    int res;

    if(recv(s,&res,sizeof(int), MSG_WAITALL) <0)
    {
        perror("Error al recibir respuesta del broker");
    }

    return res;
}
int end_clnt(void){
    return 0;
}
int subscribe(const char *tema){
    int op = SUBSCRIBE;
    int long1 = htonl(strlen(tema));
    struct iovec iov[4];
    iov[0].iov_base=&op;
    iov[0].iov_len=sizeof(int);
    iov[1].iov_base=&uuid;
    iov[1].iov_len=sizeof(UUID_t);
    iov[2].iov_base=&long1;
    iov[2].iov_len=sizeof(int);
    iov[3].iov_base=tema;
    iov[3].iov_len=strlen(tema);
    if (writev(s, iov, 4)<0)
    {
        perror("error al escribir los datos para la subscripcion");
        return 1;
    }
    int res;

    if(recv(s,&res,sizeof(int), MSG_WAITALL) <0)
    {
        perror("Error al recibir respuesta del broker");
    }
    if(res == -1)
    {
        fprintf(stderr, "Alguno de los datos introducidos es erroneo\n");
    }
    return res;
}
int unsubscribe(const char *tema){
    return 0;
}
int publish(const char *tema, const void *evento, uint32_t tam_evento){
    int op = PUBLISH;
    struct cabecera cab;
    cab.long1 = htonl(strlen(tema));
    cab.long2 = htonl(tam_evento);
    struct iovec iov[4];
    iov[0].iov_base=&op;
    iov[0].iov_len=sizeof(int);
    iov[1].iov_base=&cab;
    iov[1].iov_len=sizeof(cab);
    iov[2].iov_base=tema;
    iov[2].iov_len=strlen(tema);
    iov[3].iov_base=evento;
    iov[3].iov_len=tam_evento;
    if (writev(s, iov, 4)<0)
    {
        perror("error al escribir los datos para la subscripcion");
        return 1;
    }
    int res;

    if(recv(s,&res,sizeof(int), MSG_WAITALL) <0)
    {
        perror("Error al recibir respuesta del broker");
    }
    if(res == -1)
    {
        fprintf(stderr, "Alguno de los datos introducidos es erroneo\n");
    }
    return res;
    return 0;
}
int get(char **tema, void **evento, uint32_t *tam_evento){
    int op = GET;
    struct iovec iov[2];
    iov[0].iov_base=&op;
    iov[0].iov_len=sizeof(int);
    iov[1].iov_base=&uuid;
    iov[1].iov_len=sizeof(UUID_t);
    if (writev(s, iov, 2)<0)
    {
        perror("Error en writev");
    }
    return 0;
}

// operaciones que facilitan la depuración y la evaluación
int topics(){ // cuántos temas existen en el sistema

    int op = TOPICS;
    if(send(s, &op, sizeof(int), 0) < 0)
    {
        perror("Error al enviar el codigo de operación");
    }
    int res;
    if(recv(s, &res, sizeof(int), MSG_WAITALL) < 0)
    {
        perror("Error al recibir el número de temas");
        return -1;
    }
    return res;
}
int clients(){ // cuántos clientes existen en el sistema
    int op = CLIENTS;
    if(send(s, &op, sizeof(int), 0) < 0)
    {
        perror("Error al enviar el codigo de operación");
    }
    int res;
    if(recv(s, &res, sizeof(int), MSG_WAITALL) < 0)
    {
        perror("Error al recibir el número de clientes");
        return -1;
    }
    return res;
}
int subscribers(const char *tema){ // cuántos subscriptores tiene este tema
    int op = SUBSCRIBERS;
    int long1 = htonl(strlen(tema));
    struct iovec iov[3];
    iov[0].iov_base=&op;
    iov[0].iov_len=sizeof(int);
    iov[1].iov_base=&long1;
    iov[1].iov_len=sizeof(int);
    iov[2].iov_base=tema;
    iov[2].iov_len=strlen(tema);
    if (writev(s, iov, 3)<0)
    {
        perror("error al escribir los datos para la subscripcion");
        return 1;
    }
    int res;
    
    if(recv(s,&res,sizeof(int), MSG_WAITALL) <0)
    {
        perror("Error al recibir respuesta del broker");
    }
    if(res < 0)
    {
        fprintf(stderr, "Alguno de los datos introducidos es erroneo\n");
    }
    return res;
}
int events() { // nº eventos pendientes de recoger por este cliente
    int op = EVENTS;
    struct iovec iov[2];
    iov[0].iov_base=&op;
    iov[0].iov_len=sizeof(int);
    iov[1].iov_base=&uuid;
    iov[1].iov_len=sizeof(UUID_t);
    
    if(writev(s,iov, 2) < 0)
    {
        perror("Error en writev");
        close(s);
        return 1;
    }
    int res;
    if(recv(s, &res, sizeof(int), MSG_WAITALL) < 0)
    {
        perror("Error al recibir eventos");
        return -1;
    }
    return res;
}
