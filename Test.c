#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <stdint.h>

#define FILENAME "keyvs"
#define FILENAME_WITH_PATH "/dev/keyvs"

#define COMMAND_ID_LENGTH 3 /*SET or DEL ,3 byte */
#define MAX_KEY_LENGTH 8 /* 64 bit */
#define MAX_VAL_LENGTH 8 /* 64 bit */
#define SET_COMMAND_LENGTH (COMMAND_ID_LENGTH + MAX_KEY_LENGTH + MAX_VAL_LENGTH )
#define DEL_COMMAND_LENGTH (COMMAND_ID_LENGTH + MAX_KEY_LENGTH )
#define TRUE 1
#define FALSE 0
uint8_t insertKey(int* fd, int64_t key, int64_t val) 
{
  char commandStr[SET_COMMAND_LENGTH+1]="SET";
  int len_out;
  printf("insertKey: key=%ld, val=%ld \n", key,val);
  strncpy(&commandStr[COMMAND_ID_LENGTH], (char *)&key,MAX_KEY_LENGTH);
  strncpy(&commandStr[COMMAND_ID_LENGTH+MAX_KEY_LENGTH], (char *)&val,MAX_VAL_LENGTH);

  len_out = write(*fd, &commandStr[0], SET_COMMAND_LENGTH);

    if (SET_COMMAND_LENGTH != len_out)
    {
      switch (len_out)
      {
	case 0:
          commandStr[SET_COMMAND_LENGTH]='\0';
          printf("Key Insert Failed! err_code:%d Wrong Command:%s \n",len_out,&commandStr[0]);
	  break;
        default:
          printf("Key Insert Failed! err_code:%d\n",len_out);
	  break;
      }

      return FALSE;
    }
    else
    {
      printf("Inserted: key:%ld val:%ld \n",key,val);
      return TRUE;
    }
}

uint8_t deleteKey(int* fd, int64_t key) 
{
  char commandStr[DEL_COMMAND_LENGTH+1]="DEL";
  int len_out;
  printf("deleteKey: key=%ld \n", key);
  strncpy(&commandStr[COMMAND_ID_LENGTH], (char *)&key,MAX_KEY_LENGTH);
  len_out = write(*fd, &commandStr[0], DEL_COMMAND_LENGTH);

    if (DEL_COMMAND_LENGTH != len_out)
    {
      switch (len_out)
      {
	case 0:
          commandStr[DEL_COMMAND_LENGTH]='\0';
          printf("Key Delete Failed! err_code:%d Wrong Command:%s \n",len_out,&commandStr[0]);
	  break;
        default:
          printf("Key Delete Failed! err_code:%d\n",len_out);
	  break;
      }

      return FALSE;
    }
    else
    {
      printf("Deleted: key:%ld \n",key);
      return TRUE;
    }
}
int64_t searchKey(int* fd, int64_t key) 
{
  char commandStr[MAX_KEY_LENGTH+1];
  int len_out;
  int64_t val;
  printf("searchKey: key=%ld \n", key);
  strncpy(&commandStr[0], (char *)&key,MAX_KEY_LENGTH); 

    len_out = read(*fd, &commandStr[0], MAX_KEY_LENGTH);

    if (MAX_KEY_LENGTH != len_out)
    {
      if (-1 == len_out)
       printf("Key Not Found err_code:%d\n",len_out);
      else if(0 == len_out)
       printf("Wrong command len  err_code:%d\n",len_out);
      else
       printf("Key Search Failed! err_code:%d\n",len_out);
    }
    else
    {
      strncpy((char *)&val ,&commandStr[0], MAX_VAL_LENGTH); 
      printf("Key Found: key:%ld val:%ld \n",key,val);
      return TRUE;
    }
}
int64_t strToInt(char a[]) {
  int c, sign, offset;
  int64_t n;
  if (a[0] == '-') {  // Handle negative integers
    sign = -1;
    offset = 1;
  }
  else {
    offset = 0;
  }
 
  n = 0;
 
  for (c = offset; a[c] != '\0'; c++) {
    n = n * 10 + a[c] - '0';
  }
 
  if (sign == -1) {
    n = -n;
  }

  return n;
}
int main(int argc, char *argv[] ) {

  int fd;
  int8_t rc=0;


    if ( argc < 2 ) /* argc should be 2 for correct execution */
    {
        /* We print argv[0] assuming it is the program name */
        printf( "usage: Argument is too less \n" );
        printf( "usage: Inserting/Replacing a Key&value of size int64_t: ./test INS <key> <value>  : ./test INS 1334 4356 \n");
        printf( "usage: Searching a insearted Key: ./test GET <key> :./test  GET 1334  \n");
        printf( "usage: Deleting a Key ./test DEL <key>: ./test DEL 1334 \n");
	return 1;
    }
    else 
    {
      fd = open(FILENAME_WITH_PATH, O_RDWR);

      if (fd < 0) {
	printf("Failed to open /dev/%s: %d\n",FILENAME, fd);
	return 1;
      }

      switch(argc)
      {
      case 4: //INSERT
        if(!strncmp("INS",(const char *)&argv[1][0],3))
	  insertKey( &fd,strToInt((char *)&argv[2][0]),strToInt((char *)&argv[3][0]));
	else
          rc=-1; 
	break;
      case 3: // GET/DEL
        if(!strncmp("GET",(const char *)&argv[1][0],3))
	  searchKey( &fd,strToInt((char *)&argv[2][0]));
	else if(!strncmp("DEL",(const char *)&argv[1][0],3))
	  deleteKey( &fd,strToInt((char *)&argv[2][0]));
        else 
          rc=-1; 
	break;
      default:
	rc=-1;
      }
      close(fd);
    }
    if (rc == -1)
    {
        printf( "usage: Argument is incorrect\n" );
        printf( "usage: Inserting/Replacing a Key&value of size int64_t: ./test INS <key> <value>  : ./test INS 1334 4356 \n");
        printf( "usage: Searching a insearted Key: ./test GET <key> :./test  GET 1334  \n");
        printf( "usage: Deleting a Key ./test DEL <key>: ./test DEL 1334 \n");
    }
    return 1;
}
