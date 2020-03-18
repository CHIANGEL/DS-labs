# Distributed System Lab 1

## 设计策略

1. 策略选择和参数设置

    - 我使用GO-BACK-N的实现方法，参数设置上，我将滑动窗口的大小（window_size）设为10，超时阈值（timeout）设为0.3

    - 在checksum算法上，我选择了因特网校验法的魔改版（原始的因特网校验法似乎是生成和校验方法有些许差别，我这里统一成一个计算过程）：
        ```c
        static short Internet_Checksum(struct packet *pkt) {
            unsigned long checksum = 0; // 32位
            // 前两个字节为checksum区域，需要跳过
            for (int i = 2; i < RDT_PKTSIZE; i += 2) {
                checksum += *(short *)(&(pkt->data[i]));
            }
            while (checksum >> 16) { // 若sum的高16位非零
                checksum = (checksum >> 16) + (checksum & 0xffff);
            }
            return ~checksum;
        }
        ```

2. GO-BACK-N的策略优化

    - receiver会维护一个window_size大小的包缓存（receiver_buffer），用来存储当前这个窗口下的对应的包，以应对数据包乱序达到的情况（out of order）。

3. sender的收包&发包机制
    
    - 收包机制

        1. 对从上层来的消息，sender维护一个message_buffer，每次从上层接收到要发出去的消息后就先存入该缓存中，然后判断目前sender是否在发包（即检查timer是否正在计时），若没有则开始发包过程，若正在发包则结束收包工作。

        2. 对从下层来的ACK包，sender会首先检测收到的ack seq是否是自己目前想要的，如果是，则移动包的队列（packet_window）的队头，即go-back-n中的窗口右移。之后直接重新计时，开始新一轮的消息切割和发送数据包的操作。

    - 发包机制：

        1. sender会首先调用chunk_message函数对当前message_buffer中所有等待发送的消息全部切割成小数据包，并按顺序存储在包的队列中（packet_window）。

        2. 之后sender每次从包的队列中取前window_size个包（即当前窗户中的包）发给下层。

        3. 若计时器超时（timeout），说明仍然没有收到ACK，因此重新计时，然后重发当前窗口中的包。

3. receiver的收包&发包机制
    
    - 收包机制

        1. 对从下层来的包，receiver会直接将恰好落在当前窗口的数据包直接放入包缓存中（GO-BACK-N的优化），若接收到了想要的包，则会直接解析包的数据并写入相应的message位置，同时发回ACK包。

    - 发包机制

        1. 就是从receiver想下层发送ACK包

4. 包的结构设计

    - sender发出的数据包分为两种
        
        1. 如果这个数据包恰好是一个消息的第一个包，则payload的前四个byte表示一个整型，表示这个消息（message）的大小，方便receiver合并包的数据重构消息。
            ```
            |<-  2 byte  ->|<-  4 byte  ->|<-  1 byte  ->|<-  4 byte  ->|<-  the rest  ->|
            |   checksum   |  packet seq  | payload size | message size |     payload    |
            ```
        
        2. 如果这个数据包不是一个消息的第一个包，则payload正常表示，不包含消息大小信息。
            ```
            |<-  2 byte  ->|<-  4 byte  ->|<-  1 byte  ->|<-  the rest  ->|
            |   checksum   |  packet seq  | payload size |     payload    |
            ```
    - receiver会向下层发出ACK包。
        ```
        |<-  2 byte  ->|<-  4 byte  ->|<-  the rest  ->|
        |   checksum   |    ack seq   |   meaningless  |
        ```

5. 测试结果展示

    - 成功通过文档中的两个test case，吞吐量也在合理范围。

    ```shell
    root@iZbp1j3cfhjoinnafkaw4sZ:~/Distrubuted-System/lab1# ./rdt_sim 1000 0.1 100 0.15 0.15 0.15 0
    ## Reliable data transfer simulation with:
            simulation time is 1000.000 seconds
            average message arrival interval is 0.100 seconds
            average message size is 100 bytes
            average out-of-order delivery rate is 15.00%
            average loss rate is 15.00%
            average corrupt rate is 15.00%
            tracing level is 0
    Please review these inputs and press <enter> to proceed.

    At 0.00s: sender initializing ...
    At 0.00s: receiver initializing ...
    At 1025.78s: sender finalizing ...
    At 1025.78s: receiver finalizing ...

    ## Simulation completed at time 1025.78s with
            985245 characters sent
            985245 characters delivered
            49678 packets passed between the sender and the receiver
    ## Congratulations! This session is error-free, loss-free, and in order.
    ```

    ```shell
    root@iZbp1j3cfhjoinnafkaw4sZ:~/Distrubuted-System/lab1# ./rdt_sim 1000 0.1 100 0.3 0.3 0.3 0
    ## Reliable data transfer simulation with:
            simulation time is 1000.000 seconds
            average message arrival interval is 0.100 seconds
            average message size is 100 bytes
            average out-of-order delivery rate is 30.00%
            average loss rate is 30.00%
            average corrupt rate is 30.00%
            tracing level is 0
    Please review these inputs and press <enter> to proceed.

    At 0.00s: sender initializing ...
    At 0.00s: receiver initializing ...
    At 1842.63s: sender finalizing ...
    At 1842.63s: receiver finalizing ...

    ## Simulation completed at time 1842.63s with
            995008 characters sent
            995008 characters delivered
            61535 packets passed between the sender and the receiver
    ## Congratulations! This session is error-free, loss-free, and in order.
    ```