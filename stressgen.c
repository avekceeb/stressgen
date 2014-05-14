/*
    = Stress Generator for unix hosts =
    Daemon for producing dummy load on the host under test

    CPU Load:
        Creating N threads with tight loop ("short-circuit")
    Network Load:
        Creating N UDP clients, sending packets with size/interval given

    Other Features:
        - "Heartbeats" - sending host load info (cpu % and net traffic stats)
            to given "master host" or broadcast
        - Schedule: both cpu and net loads could be launched 
            as continuous flow (default)
            or in "pulse" mode: active and sleep periods alternate

    Some Details:
    Net:
        assuming 1 Gigabit network: 125 Mbyte/sec
        Maximum traffic is 
        Max Size Packet (65000 bytes) with 525 microsec interval:
        (1000000(usec/sec)/525(usec))*65000(bytes) = 123.760.000(bytes/sec) 

    TODO:
    - Adaptive message size
    - Add option to simulate "eating" memory
    - Add option to simulate disk I/O usage
    - FreeBSD broadcast message sent to gateway MAC instead of ff
            (? try MSG_DONTROUTE as flag in sendto)
    - Need to include host name or/and ip which sent from,
        because of NAT
    - Exclude header overhead
    - Receiving commands from master host
    - sys log on Solaris
*/
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>
#if defined(__linux__)
    #include <net/ethernet.h>
    #include <linux/if_ether.h>
    #include <netpacket/packet.h>
    #include <netinet/ether.h>
    #include <sys/ioctl.h>
    #include <net/if.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
#endif
#ifdef __FreeBSD__
    #include <netinet/in.h>
    #include <sys/sysctl.h>
    #include <sys/types.h>
    #include <vm/vm_param.h>
    #include <sys/resource.h>
#endif

/* THE CHEAPEST WAY TO OBTAIN OS NAME - USE COMPILER MACRO */
#if defined(__linux__)
#define OS_NAME_LEN 5
char *os_name = "Linux";
#endif

#if defined(__FreeBSD__)
#define OS_NAME_LEN 7
char *os_name = "FreeBSD";
#endif

#if defined(__sun)
#define OS_NAME_LEN 7
char *os_name = "Solaris";
#endif

#if !defined(__linux__) && !defined(__FreeBSD__) && !defined(__sun)
#define OS_NAME_LEN 7
char *os_name = "Unknown";
#endif

#define LOCK_FILE_NAME "/tmp/stressgen.lock"
#define VECTOR_SIZE 64
#define MAX_BYTES_PER_SEC (123760000)
#define MICROSEC_PER_SEC (1000000)
#define PING_PORT_DEFAULT 50888
#define MASTER_PORT_DEFAULT 60888
#define UDP_PING_MSG_SIZE_MAX 65000
#define RAW_PING_MSG_SIZE_MAX (ETH_DATA_LEN - 100)
#define PING_MSG_SIZE_DEFAULT 1024
#define PING_DELAY_DEFAULT 1*MICROSEC_PER_SEC
#define HEARTBEAT_DELAY_DEFAULT 10*MICROSEC_PER_SEC
/* TODO:
 * Max Packet size:  MTU - (Max IP Header Size) - (UDP Header Size) = 1500 - 60 - 8 = 1432
 * but for relyability probably it should be:
 * (min IP pack size) - (Max IP Header Size) - (UDP Header Size) = 576 - 60 - 8 = 508 ???
 */
#define STATS_SIZE 1024
#define INVALID_ADDR 0

#ifdef SYSLOGGING
#include <syslog.h>
    #define log(...) syslog(LOG_INFO, __VA_ARGS__)
#else
    #define log(...) 
#endif

#ifndef max
#define max(a,b) ((a)>(b)?(a):(b))
#endif

struct schedule {
    unsigned int active, sleep;
};

struct udp_ping_info {
    char *host;
    unsigned int port, msg_size, delay;
    unsigned int (*fill_buffer_procedure)(char*, unsigned int);
    unsigned short update_every_packet;
    struct schedule phases;
};

#if defined(__linux__)
struct raw_ping_info {
    unsigned char source_mac[ETH_ALEN], target_mac[ETH_ALEN];
    unsigned int msg_size, delay;
    unsigned int (*fill_buffer_procedure)(char*, unsigned int);
    unsigned short update_every_packet;
    struct schedule phases;
};
#endif


/* GLOBALS */
pthread_mutex_t mutex_ini = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_send = PTHREAD_MUTEX_INITIALIZER;
int lock_file;
char* stub_msg = "NOT IMPLEMENTED";
#define STUB_MSG_SIZE 15
/* bytes transmitted per second */
unsigned long int tx_speed; 

/* 2 fictive D-Link MACs for source and destination */
#if defined(__linux__)
unsigned char fictive_mac_1[ETH_ALEN] = {0x00, 0x17, 0x9A, 0x22, 0x22, 0x22};
unsigned char fictive_mac_2[ETH_ALEN] = {0x00, 0x17, 0x9A, 0x11, 0x11, 0x11};
#endif

/* HELPER PROCEDURES */
long int str2long (char* str) {
    char *power;
    long int value;
    value = strtol(str, &power, 10);
    if (!value) {
        return 0;
    }
    if (power == str) {
        return value;
    }
    if ('\0' != *power) {
        switch (power[0]) {
        /* Kilo- Mega- Giga- bytes */ 
        case 'K':
            return value<<10;
            break;
       case 'M':
            return value<<20;
            break;
        case 'G':
            return value<<30;
            break;
        /* minute hour */
        case 'm':
            return value*60;
            break;
        case 'h':
            return value*3600;
            break;
        default:
            break; 
        }
    }
    return value;
}

short int str2mac (char *str, char *mac) {
    /* TODO */
    if (mac) {
        mac[0] = 0x00; mac[1] = 0x17; mac[2] = 0x9A;
        mac[3] = 0x88; mac[4] = 0x88; mac[5] = 0x88;
    }
    return 0;
}

#if defined(__linux__)
int get_first_suitable_if() {
    char tmp[512];
    struct ifconf if_conf;
    struct ifreq *if_req;
    int sock, i = -1;
    sock = socket(AF_INET, SOCK_DGRAM, 0);
    do {
        if (0 > sock)
            break;

        if_conf.ifc_len = sizeof(tmp);
        if_conf.ifc_buf = tmp;
        if (0 > ioctl(sock, SIOCGIFCONF, &if_conf))
            break;
        if_req = if_conf.ifc_req;
        while ((char*)if_req < (char*)(if_conf.ifc_req) + if_conf.ifc_len) {
            if (0 > ioctl(sock, SIOCGIFFLAGS, if_req))
                break;
            if (!(if_req->ifr_flags & IFF_LOOPBACK) && (if_req->ifr_flags & IFF_RUNNING)) {
                if (0 <= ioctl(sock, SIOCGIFINDEX, if_req)) {
                    log("Interface selected: %s %d", if_req->ifr_name, if_req->ifr_ifindex);
                    i = if_req->ifr_ifindex;
                    break;
                }
           }
           if_req++;
        }
    } while (0);
    close(sock);
    return i;
}
#endif

/* Two procedures below implement my own
 * "home-brewed" service stop/start mechanism:
 * Create "lock-file" ; write PID in it ; acquire lock on it
 * Next instance of daemon will try to acquire lock on this file
 * Kill process with PID and rewrite file.
 * Reasons for not using standard scripts are:
 * 1) Possibility of starting by non-root
 * 2) More cross-platform
 */
void kill_previous_instance () {
    int lock_file;
    char pid_str[10];
    int pid, i;
    errno = 0;
    lock_file = open(LOCK_FILE_NAME, O_RDONLY);
    if (0 < lock_file) {
        /* lock file exists */
        if (0 != lockf(lock_file, F_TEST, 0)) {
            /* lock file exists + locked */
            /* read PID of locking process */
            memset(pid_str, 0, sizeof(pid_str));
            i = read(lock_file, pid_str, sizeof(pid_str));
            if (0 < i) {
                pid = atoi(pid_str);
                if (pid) {
                    (void)kill(pid, SIGTERM);
                    /* waiting 7 seconds for previous instance ended */
                    i = 7;    
                    while (0 < i) {
                        if (0 == lockf(lock_file, F_TEST, 0)) {
                            close(lock_file);
                            return;
                        }
                        sleep(1);
                        i -= 1;
                    }
                    close(lock_file);
                    printf ("Error killing previous instance %d\nPossibly started by other user\n", pid);
                    exit(1);
                }
            }
            /* garbage in file */
            close(lock_file);
            printf ("Error reading lock file %s\nTerminating\n", LOCK_FILE_NAME);
            exit(1);
    }
    else {
            /* lock file exists + NOT locked */
        }
    }
}

int create_pid_file() {
    char pid_str[10];
    int lock_file;
    lock_file = open(LOCK_FILE_NAME, O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR|S_IROTH|S_IWOTH|S_IRGRP|S_IWGRP);
    if (0 == lockf(lock_file, F_TLOCK, 0)) {
        memset(pid_str, 0, sizeof(pid_str));
        sprintf(pid_str, "%d", (int)getpid());
        if (0 < write(lock_file, pid_str, strlen(pid_str)))
            return lock_file;
    }
    close(lock_file);
    return 0;
}

/* FILL BUFFER WITH JUNK */
unsigned int fill_dummy(char* buf, unsigned int buf_size) {
    if (!buf || buf_size < 4) {
        return 0;
    }
    buf[0] = 'B';
    buf[1] = '\0';
    memset(buf + 2, 'D', buf_size - 3);
    buf[buf_size - 1] = '\0';
    return buf_size;
}

/* FILL BUFFER WITH HOST PERFORMANCE STATISTICS */
/*
 * Host statistic in the format:
 * {
 * char CODE - ('S' - os name; 'C' - cpu stats; 'N' - network stats)
 * char '\0'
 * char DATA[]
 * char '\0'
 * }
 */
unsigned int fill_stats(char *buf, unsigned int buf_size) {
    int fd, cpu_stat_size, net_stat_size;
    /* TODO include timestamp: time_t t = time(0); */
    /* TODO: put each stats in its own procedure */
    /* code */
    buf[0] = 'C';
    buf[1] = '\0';
    buf += 2;
    fd = -1;
#ifdef __linux__
    /* cpu stats */
    fd = open ("/proc/loadavg", O_RDONLY);
    if (-1 == fd) {
        return -2;
    }
    cpu_stat_size = read (fd, buf, buf_size - 2);
    (void) close (fd);
    if (cpu_stat_size <= 0 ) {
        return -4;
    }
#endif
#ifdef __FreeBSD__
    int mib[2];
    size_t len;
    struct loadavg lavg;
    mib[0] = CTL_VM; mib[1] = VM_LOADAVG;
    len = sizeof(struct loadavg);
    sysctl(mib, 2, &lavg, &len, 0, 0);
    cpu_stat_size = sprintf(buf, "%.2f %.2f %.2f ", 
                    ((float)lavg.ldavg[0]) / ((float)lavg.fscale),
                    ((float)lavg.ldavg[1]) / ((float)lavg.fscale),
                    ((float)lavg.ldavg[2]) / ((float)lavg.fscale ));
    if (0 >= cpu_stat_size) {
        return -1;
    }
#endif
#if !defined(__linux__) && !defined(__FreeBSD__)
    strncpy(buf, stub_msg, STUB_MSG_SIZE);
    cpu_stat_size = STUB_MSG_SIZE;
#endif
    buf[cpu_stat_size] = '\0';
    buf += cpu_stat_size + 1;
    /* network stats */
    buf[0] = 'N';
    buf[1] = '\0';
    buf += 2;
#ifdef __linux__
    fd = open ("/proc/net/dev", O_RDONLY);
    if (-1 == fd) {
        return -5;
    }
    net_stat_size = read (fd, buf, buf_size - cpu_stat_size - 5);
    (void) close (fd);
    if (net_stat_size <= 0 ) {
        return -7;
    }
#else
    strncpy(buf, stub_msg, STUB_MSG_SIZE);
    net_stat_size = STUB_MSG_SIZE;
#endif
    buf[net_stat_size] = '\0';
    buf += net_stat_size + 1;
    /* os name */
    buf[0] = 'S';
    buf[1] = '\0';
    buf += 2;
    strncpy(buf, os_name, OS_NAME_LEN);
    buf[OS_NAME_LEN + 1] = '\0';
    return cpu_stat_size + net_stat_size + OS_NAME_LEN + 3*3;
}

/* THREAD PROCEDURE FOR CPU LOAD */
void* cpuloader(void *thread_arg) {
    unsigned short i, i1, i2;
    int vector[VECTOR_SIZE];
    unsigned long int its_time = 0;
    struct schedule *sch = (struct schedule *)thread_arg;
    pthread_mutex_lock( &mutex_ini );
    srand(time(0));
    for (i = 0; i < VECTOR_SIZE; i++) {
        vector[i] = rand();
    }
    pthread_mutex_unlock( &mutex_ini );
    i = 0;
    /* eternal loop */
    while (1) {
        if (sch->sleep) {
            its_time = time(0) + sch->active;
        }
        /* active phase */
        while (sch->sleep ? (time(0) < its_time) : 1) {
            i1 = i % VECTOR_SIZE;
            i2 = (i + 1) % VECTOR_SIZE;
            vector[i1] ^= vector[i2];
            vector[i1] *= 17;
            vector[i1] |= vector[i2];
            i++;
        }
        /* sleep phase */
        if (sch->sleep) {
            sleep(sch->sleep);
        }
    }
}

/* THREAD PROCEDURE FOR SENDING UDP PACKETS */
void* udp_sender (void *thread_arg) {
    const int set_on = 1;
    unsigned long int packet_size, its_time = 0;
    struct hostent *he;
    struct sockaddr_in sa;
    int sock;
    char * payload;
    struct udp_ping_info* info = (struct udp_ping_info*)thread_arg;
    pthread_mutex_lock( &mutex_ini );
    sock = socket(PF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        log("ERROR: create socket");
        return 0;
    }
    /* filling socket address structure */
    memset((char *)&sa, 0, sizeof(struct sockaddr_in));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(info->port);
    /* special treat for host name == 0 => SEND BROADCAST */
    if (0 == info->host) {
        sa.sin_addr.s_addr = INADDR_BROADCAST;
    }
    else {
        he = gethostbyname(info->host);
        if ( 0 == ((char *)he) ) {
                log("ERROR: Invalid host %s\n", info->host);
                return 0;
        }
        /* TODO: check if there are more than 1 address */
        /* #define h_addr  h_addr_list[0]  for backward compatibility */
         memcpy(&(sa.sin_addr), he->h_addr, he->h_length);
    }
    if (sa.sin_addr.s_addr == INADDR_BROADCAST) {
        setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &set_on, sizeof(set_on));
    }
    payload = (char*)malloc(info->msg_size);
    packet_size = (info->fill_buffer_procedure)(payload, info->msg_size);
    pthread_mutex_unlock( &mutex_ini );

    /* eternal loop */
    while (1) {
        if (info->phases.sleep) {
            its_time = time(0) + info->phases.active;
        }
        while (info->phases.sleep ? (time(0) < its_time) : 1) {
            pthread_mutex_lock( &mutex_send );
            (void)sendto(sock, payload, packet_size,
                0, (struct sockaddr *) &sa, sizeof(struct sockaddr));
            pthread_mutex_unlock( &mutex_send );
            usleep(info->delay);
            if (info->update_every_packet) {
                packet_size = (info->fill_buffer_procedure)(payload, info->msg_size);
            }
        }
        if (info->phases.sleep) {
            sleep(info->phases.sleep);
        }
    }
    close(sock);
    free(payload);
}

/* THREAD PROCEDURE FOR SENDING RAW ETHERNET PACKETS */
#if defined(__linux__)
void* raw_sender (void *thread_arg) {
    struct sockaddr_ll target_addr;
    int raw_sock = 0, if_index;
    unsigned long int packet_size, its_time = 0;
    char *packet, *payload;
    struct ethhdr *packet_header;

    struct raw_ping_info *info = (struct raw_ping_info*)thread_arg;
    packet = (char*) malloc(info->msg_size + ETH_HLEN);
    packet_header = (struct ethhdr *)packet;
    payload = packet + ETH_HLEN;
    if (0 > (raw_sock = socket(PF_PACKET, SOCK_RAW, htons(ETH_P_ALL)))) {
        log("socket() Error #%d: %s\n", errno, strerror(errno));
        return 0;
    }
    /* fill address */
    memset(&target_addr, 0, sizeof (struct sockaddr_ll));
    target_addr.sll_family = PF_PACKET;
    target_addr.sll_protocol = htons(ETH_P_IP);
    /* index of the network device lo - 1; eth0 - 2
     * TODO: calculate index */
    if_index = get_first_suitable_if();
    if (0 > if_index) {
        log("Error: no suitable (RUNNING) iface found");
        return 0;
    }
    target_addr.sll_ifindex = if_index;
    // ARP hardware identifier is ethernet
    target_addr.sll_hatype = ARPHRD_ETHER;
    // target is another host
    target_addr.sll_pkttype = PACKET_OTHERHOST;
    //address length
    target_addr.sll_halen = ETH_ALEN;
    memcpy(&(target_addr.sll_addr), info->target_mac, ETH_ALEN);

    /* Ethernet Packet Header*/
    memcpy(packet, info->target_mac, ETH_ALEN);
    memcpy((packet + ETH_ALEN), info->source_mac, ETH_ALEN);

    /* using non-existing protocol 8200 instead of ETH_P_IP */
    packet_header->h_proto = htons(0x8200);
    /* User data */
    packet_size = (info->fill_buffer_procedure)(payload, info->msg_size);
    packet_size += ETH_HLEN;

    /* eternal loop */
    while (1) {
        if (info->phases.sleep) {
            its_time = time(0) + info->phases.active;
        }
        while (info->phases.sleep ? (time(0) < its_time) : 1) {
            pthread_mutex_lock( &mutex_send );
            (void)sendto(raw_sock, packet, packet_size,
                0, (struct sockaddr *) &(target_addr), sizeof (struct sockaddr_ll));
            pthread_mutex_unlock( &mutex_send );
            usleep(info->delay);
            if (info->update_every_packet) {
                packet_size = (info->fill_buffer_procedure)(payload, info->msg_size);
            }
        }
        if (info->phases.sleep) {
            sleep(info->phases.sleep);
        }
    }
    close(raw_sock);
    free(payload);
    return (0);
}
#endif

void signal_handler(int sgn) {
    /* TODO: implement cleanup - stopping threads, closing sockets etc */
     if (0 == lockf(lock_file, F_ULOCK, 0))
        close(lock_file);
     exit(0);
}

/* PARSE PARAMETERS; DEMONIZE;  START THREADS */
int main(int argc, char** argv) {
    pid_t pid, sid;
    pthread_t *thread_pool, *thread;

    struct udp_ping_info *udp_pinger_pool = 0, *udp_pinger = 0;
    struct schedule *cpu_schedule_pool = 0, *cpu_schedule = 0;

#if defined(__linux__)
    struct raw_ping_info *raw_pinger = 0;
    unsigned short int raw_ping = 0;
#endif

    int op, rc, i, hb=0, cpu=0, ping=0, thread_pool_size=0, socket_pool_size=0;
    unsigned int ping_msg_size = PING_MSG_SIZE_DEFAULT;
    int ping_delay = PING_DELAY_DEFAULT;
    int master_port = MASTER_PORT_DEFAULT;
    int ping_port = PING_PORT_DEFAULT;
    char* master_host = 0;
    int heartbeat_delay = HEARTBEAT_DELAY_DEFAULT;
    unsigned int active_period = 0;
    unsigned int sleep_period = 0;
#define RANDOM_START 1
#define ALTERNATE_LOAD 2
    unsigned short int shuffle_phases = 0;
    opterr = 0;
    if (1 == argc) {
        printf("Usage: %s [options] [hosts]\n"
        "   Where options are:\n"
        "       -X                   Stop Daemon\n"
        "       -C<threads>          CPU Load\n"
        "       -N<Bytes/sec>[K|M]   Net Load\n"
#if defined (__linux__)
        "       -E                   Use Ethernet packets (only root)\n"
#endif
        "   Schedule options:\n"
        "       -A<seconds>[m|h]     Active phase duration\n"
        "       -S<seconds>[m|h]     Sleep phase duration\n"
        "       -I                   Alternate CPU and Net loads in turn\n"
        "       -R                   Random mix of CPU and Net phases\n"
        "   Heartbeat options:\n"
        "       -M<host>             Send heartbeats to master host\n"
        "       -B                   Send heartbeats broadcast\n\n"
        "   'K'=KiB; 'M'=MiB; 'm'=minute; 'h'=hour\n\n"
        "   'hosts' - list of hosts to direct net load to\n"
        , argv[0]);
        return 0;
    }
    /* parsing named cmd line parameters */
    while (-1 != (op = getopt (argc, argv, "C:N:BM:S:A:RIXEm:p:s:d:h:"))) {
        switch (op) {
        /* main options */
        case 'C':
            cpu = atoi(optarg);
            break;
        case 'N':
            tx_speed = str2long(optarg);
            break;
        case 'B':
            master_host = 0;
            hb = 1;
            break;
        case 'M':
            master_host = optarg;
            hb = 1;
            break;
        case 'S':
            sleep_period = (unsigned int)str2long(optarg);
            break;
        case 'A':
            active_period = (unsigned int)str2long(optarg);
            break;
        case 'R':
            shuffle_phases = RANDOM_START;
            break;
        case 'I':
            shuffle_phases = ALTERNATE_LOAD;
            break;
#if defined(__linux__)
        case 'E':
            raw_ping = 1;
            break;
#endif
        case 'X':
            kill_previous_instance();
            return 0;
            break;
        /* fine tuning options */
        case 'm':
            master_port = atoi(optarg);
            break;
        case 'p':
            ping_port = atoi(optarg);
            break;
        case 's':
            ping_msg_size = (unsigned int)str2long(optarg);
            break;
        case 'd':
            ping_delay = atoi(optarg);
            break;
        case 'h':
            heartbeat_delay = MICROSEC_PER_SEC*(unsigned int)str2long(optarg);
            break;
        default:
            break;
        }
    }
    /* the rest of cmd line - hostnames */
#if defined(__linux__)
    if (!raw_ping)
#endif
        ping = argc - optind;
    thread_pool_size = hb + cpu + ping;

#if defined(__linux__)
    thread_pool_size += raw_ping;
#endif

    socket_pool_size = ping + hb;

    if (!thread_pool_size) {
        return 0;
    }

    /* parameter validation */
#if defined(__linux__)
    if (raw_ping && (0 != geteuid())) {
        printf("Error: You must be root to use raw sockets\nTry without -E\n");
        return 1;
    }
#endif
    /* if one of phase is omitted, use equal periods */
    if ( (!active_period) && sleep_period ) {
        active_period = sleep_period;
    } 
    if ( active_period && (!sleep_period) ) {
        sleep_period = active_period;
    }
    if (ping_msg_size > UDP_PING_MSG_SIZE_MAX || ping_msg_size <=0 ) {
        ping_msg_size = PING_MSG_SIZE_DEFAULT;
    }
    if (ping_port > 65535) {
        /* Strictly, port number should be > 49151
            to not overlap with the registered ones */
        ping_port = PING_PORT_DEFAULT;
    }
    if (ping_delay <= 0)
        ping_delay = PING_DELAY_DEFAULT;

     /* if 'T' option set => override msg_size and delay */
    if (0 < tx_speed) {
        if (MAX_BYTES_PER_SEC < tx_speed)
            tx_speed = MAX_BYTES_PER_SEC;
#if defined(__linux__)
        if (raw_ping) {
        ping_msg_size = RAW_PING_MSG_SIZE_MAX;
        ping_delay = (int)((float)MICROSEC_PER_SEC/tx_speed*RAW_PING_MSG_SIZE_MAX);
        }
        else
#endif
        {
        ping_msg_size = UDP_PING_MSG_SIZE_MAX;
        ping_delay = (int)((float)MICROSEC_PER_SEC/tx_speed*UDP_PING_MSG_SIZE_MAX);
        }
    }

    pid = fork();
    if (pid < 0) {
        exit(1);
    }
    if (pid > 0) {
        exit(0);
    }
    sid = setsid();
    if (sid < 0) {
        exit(2);
    }

    umask(0);

    kill_previous_instance();

    lock_file = create_pid_file();
    if(!lock_file) {
        return 1;
    }

    signal(SIGTERM, signal_handler);

#if defined(SYSLOGGING)
    openlog("stressgen", LOG_PID, LOG_DAEMON);
#endif

    /* Recycle procedure ends. Work (if any) starts */
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);

    log("Params:"
        "Tx=%ld (B/sec) Delay=%d(usec) Msg=%d(B)  Active=%d(sec) Sleep=%d(sec)",
            tx_speed, ping_delay, ping_msg_size, active_period, sleep_period);

    thread = thread_pool = (pthread_t*) malloc(thread_pool_size * sizeof(pthread_t));
    if (cpu)
        cpu_schedule = cpu_schedule_pool = (struct schedule*) malloc(cpu * sizeof(struct schedule));
    if (socket_pool_size)
        udp_pinger = udp_pinger_pool = (struct udp_ping_info*) malloc(socket_pool_size * sizeof(struct udp_ping_info));

#if defined (__linux__)
    if (raw_ping)
        raw_pinger = (struct raw_ping_info*) malloc(sizeof(struct raw_ping_info));
#endif

    /* start heartbeats to master host with stats in payload */
    if (1 == hb) {
        log("Starting heartbeats");
        udp_pinger->fill_buffer_procedure = fill_stats;
        udp_pinger->msg_size = STATS_SIZE;
        udp_pinger->delay = heartbeat_delay;
        udp_pinger->phases.sleep = 0;
        udp_pinger->phases.active = 0;
        udp_pinger->host = master_host;
        udp_pinger->port = master_port;
        udp_pinger->update_every_packet = 1;
        rc = pthread_create(thread, 0, udp_sender, (void*) udp_pinger);
        if (rc) {
            /* TODO */
        }
        thread++;
        udp_pinger++;
    }

    /* start cpu threads */
    for (i=0; i<cpu; i++) {
        log("Starting cpu thread # %d", i);
        cpu_schedule->active = active_period;
        cpu_schedule->sleep = sleep_period;
        rc = pthread_create(thread, 0, cpuloader, (void*)cpu_schedule);
        if (rc) {
            /* TODO */
        }
        thread++;
        cpu_schedule++; 
    }

    /* make CPU and NET loads out of sync randomly */
    if ((thread_pool_size>cpu) && (RANDOM_START == shuffle_phases) && active_period) {
        pthread_mutex_lock( &mutex_ini );
        srand(time(0));
        i = (unsigned int)((float)active_period/RAND_MAX*rand());
        pthread_mutex_unlock( &mutex_ini );
        sleep(i);
    }
    /* CPU and NET loads taken in turn */
    if ((thread_pool_size>cpu) && (ALTERNATE_LOAD == shuffle_phases) && active_period) {
        sleep(active_period);
        i = sleep_period;
        sleep_period = active_period;
        active_period = i;
    }

    /* start ping threads */
    for (i=0; i<ping; i++) {
        log("Starting ping thread # %d", i);
        udp_pinger->fill_buffer_procedure = fill_dummy;
        udp_pinger->msg_size = ping_msg_size;
        udp_pinger->delay = ping_delay;
        udp_pinger->phases.sleep = sleep_period;
        udp_pinger->phases.active = active_period;
        udp_pinger->host = argv[optind];
        udp_pinger->port = ping_port;
        udp_pinger->update_every_packet = 0;
        rc = pthread_create(thread, 0, udp_sender, (void*) udp_pinger);
        if (rc) {
            /* TODO */
        }
        udp_pinger++;
        thread++;
        optind++;
    }

#if defined(__linux__)
    if (raw_ping) {
        log("Starting raw ethernet ping thread");
        memcpy(raw_pinger->source_mac, fictive_mac_1, ETH_ALEN);
        memcpy(raw_pinger->target_mac, fictive_mac_2, ETH_ALEN);
        raw_pinger->delay = ping_delay;
        raw_pinger->fill_buffer_procedure = fill_dummy;
        raw_pinger->update_every_packet = 0;
        raw_pinger->msg_size = ping_msg_size;
        raw_pinger->phases.active = active_period;
        raw_pinger->phases.sleep = sleep_period;
        rc = pthread_create(thread, 0, raw_sender, (void*) raw_pinger);
        if (rc) {
            /* TODO */
        }
        thread++;
    }
#endif
    /* join all threads */
    for (i=0; i<thread_pool_size; i++) {
        rc = pthread_join(thread_pool[i], 0);
        if (rc) {
            /* TODO */
        }
    }
#if defined(SYSLOGGING)
    closelog();
#endif
    free(cpu_schedule_pool);
    free(udp_pinger_pool);
#if defined(__linux__)
    free(raw_pinger);
#endif
    free(thread_pool);
    return 0;
}

