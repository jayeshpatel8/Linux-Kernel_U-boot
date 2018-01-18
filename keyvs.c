/*
 *  Creates a read-write char device for key-value store
 */
#include "linux/module.h"
#include "linux/string.h"
#include "asm/uaccess.h"
#include "linux/fs.h"
#include "linux/cdev.h"
#include "linux/slab.h"
#include "linux/semaphore.h"
#include "linux/mutex.h"
#include <linux/sched.h>

//#define  DEBUG 1

#define MODNAME "Keyvs"
#define printd(...) printk(MODNAME ": " __VA_ARGS__)


MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("key-val-store - ram char dev driver");

#define COMMAND_ID_LENGTH 3 /*SET or DEL ,3 byte */
#define MAX_KEY_LENGTH 8 /* 64 bit */
#define MAX_VAL_LENGTH 8 /* 64 bit */
#define SET_COMMAND_LENGTH (COMMAND_ID_LENGTH + MAX_KEY_LENGTH + MAX_VAL_LENGTH  )
#define DEL_COMMAND_LENGTH (COMMAND_ID_LENGTH + MAX_KEY_LENGTH )

#define MAX_KEY_SUPPORTED 256 


/* Tree Node Data structure  */
typedef struct node
{
    int64_t key;
    int64_t data;    

    uint64_t height;

    struct node* left;
    struct node* right;
}t_node;


/*device specific data structure*/
static int major = 61;
static int minor = 0;
static int range = 1;
static dev_t majorminor;
struct cdev kvs_cdev;

/*Tree related data structure*/
static t_node* p_treeRoot; /*root of the tree */
static t_node* p_freeNodePool; /*free node pool collects the node memory Deleting the key */

static atomic_t totalLiveKeyCount = ATOMIC_INIT(0); /*total live key in tree */
static atomic_t rotationDueToInsertionCount = ATOMIC_INIT(0); /*total counts */
struct semaphore keyInsDelLock; /*binary semphore for locking */

static	int64_t key;
static  char tempString[SET_COMMAND_LENGTH];

/*Static functions*/
static int kvs_open (struct inode *, struct file *);
static int kvs_release (struct inode *, struct file *);
static ssize_t kvs_read (struct file *, char *, size_t, loff_t *);
static ssize_t kvs_write (struct file *, const char *, size_t, loff_t *);

static void deleteCompeleteTree(t_node* p_node);
static t_node* searchKey(t_node* p_node, int64_t key);
static t_node* insertKey(t_node* p_node, int64_t key, int64_t data);
static t_node* deleteKey(t_node* p_node, int64_t key);

static t_node* addNewNode(int64_t key, int64_t data);
static uint64_t findMaxValue(uint64_t a, uint64_t b);
static uint64_t getHeightOfNode(t_node* p_node);
static void updateNewHeightOfNode(t_node* p_node);
#ifdef DEBUG
  static void printTreeInPreOrder( t_node *root);
#endif
static t_node* rotateNodeRight(t_node* p_yNode);
static t_node* rotateNodeLeft(t_node* p_yNode);
static t_node* rebalanceTheTree(t_node* p_node);
static t_node* findMinNodeFromSubtTree(t_node* p_node);
static t_node* RemoveMinNodeLinkFromSubTree(t_node* p_node);
static void freeTree(t_node* p_node);

/*File operation Data structure*/
struct file_operations kvs_fops = {
	owner:	THIS_MODULE,
	open:	kvs_open,
	release: kvs_release,
	read: 	kvs_read,
	write: 	kvs_write,
};


/* Module Init function*/
static int __init kvs_init(void) 
{
	int rc;
	p_treeRoot = NULL;
	printd("init\n");
	p_freeNodePool = NULL;

	sema_init(&keyInsDelLock,1);
	majorminor = MKDEV(major, minor);
	rc = register_chrdev_region(majorminor, range, MODNAME);
	if (rc != 0) {
		printd("failed to register %d\n", rc);
		return rc;
	}

	printd("major: %d - minor: %d\n", major, minor);

	cdev_init(&kvs_cdev, &kvs_fops);
	cdev_add(&kvs_cdev, majorminor, range);

	return 0;

}
module_init(kvs_init);
/* Module Exit function*/
static void __exit kvs_exit(void) 
{
    printd("exiting\n");
    cdev_del(&kvs_cdev);
    unregister_chrdev_region(majorminor, range);

    deleteCompeleteTree(p_treeRoot);
}
module_exit(kvs_exit);

/* 
 * Called when a process open the device file.
 */
static int kvs_open (struct inode *pinode, struct file *pfile) 
{
  printd("Max supported key is %d \n", MAX_KEY_SUPPORTED);
#ifdef DEBUG
   printd("Total Live key is %d \n",  atomic_read(&totalLiveKeyCount));
#endif 
 return 0;
}
/* 
 * Called when a process closes the device file.
 */
static int kvs_release(struct inode *pinode, struct file *pfile)
{
  return 0;
}

/* 
 * Called when a process reads the device file.
 */
static ssize_t kvs_read(struct file *pfile, char* pbuf, size_t len, loff_t *p_off)
{

	int rc;
	int treeRotationCount;

	printd("entering read\n");

	if (p_treeRoot == NULL){
          printd("No any key generate \n");
	  return -ENOKEY;
	}
        if (len == (MAX_KEY_LENGTH))
	  {
             t_node* p_keyNode;
             rc =copy_from_user(&tempString[0], pbuf, len);

	     if (rc <0) {
                printd("read key copy_from_user failed rc: %d\n", rc);
		return -EFAULT;
	     }
	     key = *(int64_t *)&tempString[0];
#ifdef DEBUG
	     printd("\n preoder \n");
	     printTreeInPreOrder( p_treeRoot);
#endif
           treeRotationCount = atomic_read(&rotationDueToInsertionCount); 
           p_keyNode = searchKey(p_treeRoot, key);
	   if (p_keyNode)
           {
               uint8_t i=0;
	       if(treeRotationCount !=  atomic_read(&rotationDueToInsertionCount))
		 {
		   //ignore the result as tree was updated/rotated by insertion;
		   return -EAGAIN;
		 }
               while(i<MAX_VAL_LENGTH)
	       {
	          tempString[i] = *(((char *)&p_keyNode->data)+i);
		  i++;
	       }

	       rc = copy_to_user(pbuf, &tempString[0], (MAX_VAL_LENGTH));

	       if (rc <0) {
                    printd("val copy_to_user failed rc: %d\n", rc);
		    return -EFAULT;
	       }	
	       

           }
	   else // key not found
	   {
	     if(treeRotationCount !=  atomic_read(&rotationDueToInsertionCount))
	     {
		   //ignore the result as tree was updated/rotated by insertion;
		   return -EAGAIN;
	     }
             else
               printd(" Key not found \n");
	     return -ENOKEY;
	   }
        }
	else
	  {
             // Error Case
             printd(" Wrong Search Key Len \n");
             return 0;
	  }

	return MAX_VAL_LENGTH;
}

/* 
 * Called when a process writes to the device file.
 */
static ssize_t kvs_write(struct file* pfile,const char *pbuf, size_t len, loff_t* p_off)
{
	int rc;

	printd("entering write\n");

	// 'SET'/'DEL'+64bitkey+64bitVal=19bytes
	if ((len != SET_COMMAND_LENGTH) && (DEL_COMMAND_LENGTH != len))
	  {
                printd("Wrong Write command: len %ld\n",len);
		return 0;
	  }
	rc = copy_from_user(&tempString[0], pbuf, len);

	if (rc <0) {
                printd("write copy_from_user failed rc: %d\n", rc);
		return -EFAULT;
	}
	// check for SET/DEL 
	if((tempString[0]=='S') && (tempString[1]=='E') && (tempString[2]=='T'))
	  {
             t_node* p_newTreeRoot;
             if (len != SET_COMMAND_LENGTH){
                printd("Wrong SET command len: %ld\n",len);
	      	return 0;
	     }
#ifdef DEBUG
	     printd("\n preoder \n");
             printTreeInPreOrder( p_treeRoot);
#endif
            if (down_interruptible(&keyInsDelLock))        
              return -ERESTARTSYS;

            if ( atomic_add_return(1,&totalLiveKeyCount) >= MAX_KEY_SUPPORTED)
            {
	      atomic_dec(&totalLiveKeyCount);
	      // MAX supported live key is reached
	      return -EXFULL;
            }

             p_newTreeRoot = insertKey(p_treeRoot, *(int64_t *)&tempString[COMMAND_ID_LENGTH] /* int64_t key*/, *(int64_t *)&tempString[COMMAND_ID_LENGTH+MAX_KEY_LENGTH]/* int64_t data */);

	     if (p_newTreeRoot){
	       atomic_inc(&totalLiveKeyCount);
	       p_treeRoot = p_newTreeRoot;
	       up(&keyInsDelLock);
	     }
	     else
             {
               up(&keyInsDelLock);
               printd("kMalloc failed rc: %d\n",-ENOMEM);
	       return -ENOMEM;
	     }
#ifdef DEBUG
	     printd("\n preoder \n");
             printTreeInPreOrder( p_treeRoot);
#endif
	  } 
	else if((tempString[0]=='D') && (tempString[1]=='E') && (tempString[2]=='L'))
	  {
#ifdef DEBUG
	     printd("\n preoder \n");
             printTreeInPreOrder( p_treeRoot);
#endif
            if (len != DEL_COMMAND_LENGTH){
                printd("Wrong DEL command len: %ld\n",len);
	      	return 0;
	    }

            if (down_interruptible(&keyInsDelLock))        
              return -ERESTARTSYS;

	    p_treeRoot = deleteKey(p_treeRoot,  *(int64_t *)&tempString[COMMAND_ID_LENGTH]/* int64_t key */);

	    up(&keyInsDelLock);

#ifdef DEBUG
	     printd("\n preoder \n");
             printTreeInPreOrder( p_treeRoot);
#endif
	  }
	else
	  {
            printd("Invalid Write command \n");
	    return -ENOMSG;
          }
	return len;
}
/************************TREE Specific implementation***************************************/
/*Search the Key */
static t_node* searchKey(t_node* p_node, int64_t key)
{
    if ( !p_node )
        return NULL;

    if ( key < p_node -> key )
        return searchKey(p_node -> left, key);
    else if ( key > p_node -> key )
        return searchKey(p_node -> right, key);
    else
        return p_node;        
}
/*Delete the key*/
static t_node* insertKey(t_node* p_node, int64_t key, int64_t data)
{
  t_node* p_return_node;
    if ( !p_node )
    {
      return addNewNode(key, data);
    }
    printk(KERN_INFO "The process is \"%s\" (pid %i) \n",        current->comm, current->pid); 

    if ( key < p_node -> key )
    {
        p_return_node = insertKey(p_node -> left, key, data);
	if (NULL == p_return_node)
	  return p_return_node;
        else{
          p_node -> left = p_return_node;
	}
    }
    else if ( key > p_node -> key )
    {
        p_return_node = insertKey(p_node -> right, key, data);
	if (NULL == p_return_node)
	  return p_return_node;
        else
          p_node -> right = p_return_node;
    }
    else 
        p_node -> data = data;

    return rebalanceTheTree(p_node);
}

/*Delete key */
static t_node* deleteKey(t_node* p_node, int64_t key)
{
    if ( !p_node )
        return NULL;

    if ( key < p_node -> key )
        p_node -> left = deleteKey(p_node -> left, key);
    else if ( key > p_node -> key )
        p_node -> right = deleteKey(p_node -> right, key);
    else
    {
        t_node* left = p_node -> left;
        t_node* right = p_node -> right;
	t_node* min_node;

        while (p_freeNodePool)
          p_freeNodePool=p_freeNodePool->right;

        p_freeNodePool=p_node;
	p_freeNodePool->right = NULL;

	atomic_dec(&totalLiveKeyCount);

        /* case: only one child*/
        if ( right == NULL )
            return left;
            
       /* case: two children */
        min_node = findMinNodeFromSubtTree(right);
        min_node -> right = RemoveMinNodeLinkFromSubTree(right);    
        min_node -> left = left;      

        return rebalanceTheTree(min_node);
    }

    return rebalanceTheTree(p_node);
}
/*Free the compelete Tree = Tree + freeNodePool */
static void deleteCompeleteTree(t_node* p_node)
{
  t_node* tempNode;

  if (p_node)
    freeTree(p_node);

  while (p_freeNodePool)
  {
    tempNode=p_freeNodePool->right;
    kfree(p_freeNodePool);
    p_freeNodePool = tempNode;
  }
}
/*Free the Tree*/
static void freeTree(t_node* p_node)
{
    if ( !p_node )
        return;
//
    freeTree(p_node -> left);
    freeTree(p_node -> right);
    kfree(p_node);
}
/* Allocated a meory for new node*/
static t_node* addNewNode(int64_t key, int64_t data)
{
  t_node* p_node;
    if (p_freeNodePool)
    {
      p_node = p_freeNodePool;
      p_freeNodePool = p_freeNodePool->right;//using right child as next free node;
    }
    else
    {
      p_node =  (t_node*) kmalloc(sizeof(t_node), GFP_KERNEL);
    }
    if (NULL == p_node)
    {
      printd("addNewNode: kmalloc Failed\n");
    }
    else
    {
      p_node -> key    = key;
      p_node -> data   = data;
      p_node -> height = 1;
      p_node -> left   = NULL;
      p_node -> right  = NULL;
    }
    return p_node;
}
/*find max value from a & b*/
static uint64_t findMaxValue(uint64_t a, uint64_t b)
{
    if (a > b)
      return a;
    else
      return b;
}
/*get height of Node*/
static uint64_t getHeightOfNode(t_node* p_node)
{
    return p_node ? p_node -> height : 0;
}

/* Update the new height of node*/
static void updateNewHeightOfNode(t_node* p_node)
{
    p_node -> height = 1 + findMaxValue(getHeightOfNode(p_node -> left), getHeightOfNode(p_node -> right));
}
/* find min mode from sub tree*/
static t_node* findMinNodeFromSubtTree(t_node* p_node)
{
    if ( p_node -> left != NULL )
        return findMinNodeFromSubtTree(p_node -> left);
    else
        return p_node;
}
/*Remove min node link from sub tree*/
static t_node* RemoveMinNodeLinkFromSubTree(t_node* p_node)
{
    if ( p_node -> left == NULL )
        return p_node -> right;

    p_node -> left = RemoveMinNodeLinkFromSubTree(p_node -> left);
    return rebalanceTheTree(p_node);
}
/* Perfporm rotate node right on tree */
static t_node* rotateNodeRight(t_node* p_yNode)
{
    t_node* p_xNode = p_yNode -> left;

    p_yNode -> left = p_xNode -> right;
    p_xNode -> right = p_yNode;

    updateNewHeightOfNode(p_yNode);
    updateNewHeightOfNode(p_xNode);

    return p_xNode;
}
/* Perfporm rotate node left on tree */
static t_node* rotateNodeLeft(t_node* p_yNode)
{
    t_node* p_xNode = p_yNode -> right;
    p_yNode -> right = p_xNode -> left;
    p_xNode -> left = p_yNode;

    updateNewHeightOfNode(p_yNode);
    updateNewHeightOfNode(p_xNode);

    return p_xNode;
}
/* Rebalance the Tree */
static t_node* rebalanceTheTree(t_node* p_node)
{
    updateNewHeightOfNode(p_node);

    if ( getHeightOfNode(p_node -> left) - getHeightOfNode(p_node -> right) == 2 )
    {
	atomic_inc(&rotationDueToInsertionCount);
        if ( getHeightOfNode(p_node -> left -> right) > getHeightOfNode(p_node -> left -> left) )
            p_node -> left = rotateNodeLeft(p_node -> left);
        return rotateNodeRight(p_node);
    }
    else if ( getHeightOfNode(p_node -> right) - getHeightOfNode(p_node -> left) == 2 )
    {
	atomic_inc(&rotationDueToInsertionCount);
        if ( getHeightOfNode(p_node -> right -> left) > getHeightOfNode(p_node -> right -> right) )
            p_node -> right = rotateNodeRight(p_node -> right);
        return rotateNodeLeft(p_node);
    }

    return p_node;
}

#ifdef DEBUG
static void printTreeInPreOrder( t_node *root)
{
    if(root != NULL)
    {
      printd("%ld ", (long)root->key);
        printTreeInPreOrder(root->left);
        printTreeInPreOrder(root->right);
    }
}
#endif
