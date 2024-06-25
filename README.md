# MIT-6.5840

Solution to MIT 6.5840: Distributed Systems Lab 2023 and 2024.

## Codes

|               Lab                |                     Source Code Folder                      |                          2023 Page                           |                          2024 Page                           |
| :------------------------------: | :---------------------------------------------------------: | :----------------------------------------------------------: | :----------------------------------------------------------: |
|            MapReduce             |                       [mr](./src/mr)                        | [2023 Lab 1](http://nil.csail.mit.edu/6.5840/2023/labs/lab-mr.html) | [2024 Lab 1](http://nil.csail.mit.edu/6.5840/2024/labs/lab-mr.html) |
|         Key/Value Server         |                    [kvsrv](./src/kvsrv)                     |                              -                               | [2024 Lab 2](http://nil.csail.mit.edu/6.5840/2024/labs/lab-kvsrv.html) |
|               Raft               |                     [raft](./src/raft)                      | [2023 Lab 2](http://nil.csail.mit.edu/6.5840/2023/labs/lab-raft.html) | [2024 Lab 3](http://nil.csail.mit.edu/6.5840/2024/labs/lab-raft.html) |
| Fault-tolerant Key/Value Service |                   [kvraft](./src/kvraft)                    | [2023 Lab 3](http://nil.csail.mit.edu/6.5840/2023/labs/lab-kvraft.html) | [2024 Lab 4](http://nil.csail.mit.edu/6.5840/2024/labs/lab-kvraft.html) |
|    Sharded Key/Value Service     | [shardctrler](./src/shardctrler) & [shardkv](./src/shardkv) | [2023 Lab 4](http://nil.csail.mit.edu/6.5840/2023/labs/lab-shard.html) | [2024 Lab 5](http://nil.csail.mit.edu/6.5840/2024/labs/lab-shard.html) |

**NOTE:** With the exception of the lab "Key/Value Server," which is based on the code of 2024, the skeleton code and tests are based on the code of 2023.

## Tricks for Debugging

+ Some tests may generate random numbers as parameters, such as raft log commands, which makes it difficult for us to read the logs for debugging. And we can use a sequential number generator `genInt` to replace the function `rand.Int`:

  ```go
  number := 100
  genInt := func() int {
      number++
      return number
  }
  ```

+ The same for the `randstring` in the Lab "Sharded Key/Value Service" Part B:

  ```go
  var number int32 = 100
  genStr := func() string {
  	return strconv.Itoa(int(atomic.AddInt32(&number, 1)))
  }
  ```

## Notes & Acknowledgements

+ Thanks to <https://www.cnblogs.com/lawliet12/p/16972383.html>, which reminded me to ignore the reply of `AppendEntries` if the current term has changed during the RPC call (`rf.currentTerm != args.Term`), so that I passed the "Figure 8 (unreliable)" test in Lab 2C. And so does the RPC `RequestVote`. See the commit [7566a372](https://github.com/Timothy-Liuxf/MIT-6.5840/commit/7566a372671f3b1c2c611b6b309e71c878072e49) and [375c20cb](https://github.com/Timothy-Liuxf/MIT-6.5840/commit/375c20cb8038190be4f1500292a10db4e60254d0) for details.
+ The server should check if it is the leader before sending `AppendEntries` RPC. See the commit [e76e8189](https://github.com/Timothy-Liuxf/MIT-6.5840/commit/e76e818952e97abd11baf08d28493c14431810e2) for details.
+ Thanks to <https://github.com/niebayes/MIT-6.5840>, which reminded me that the log cannot be truncated in `AppendEntries` if the log entries in the argument have the same term, since this RPC might have been sent long ago, thus causing log entries to be lost. For example, if a server has log entries "1 1 2 2 2 2" and receives log entries "1 1 2 2", it should keep its log entries instead of truncating them. See the commit [28c87fb8](https://github.com/Timothy-Liuxf/MIT-6.5840/commit/28c87fb847e76ae9660a5792e169a9f7c5e3aeae) for details.

## References

All online links are cited on June 25, 2024.

+ <https://github.com/RohanDoshi21/MIT-6.5840-Distributed-Systems>
+ <https://github.com/niebayes/MIT-6.5840>
+ <https://www.cnblogs.com/lawliet12/p/16972383.html>
+ <https://juejin.cn/post/7330143281525391414>
+ <https://github.com/lawliet9712/MIT-6.824>
+ <https://github.com/zhuqiweigit/MIT6.824>
+ <https://github.com/niebayes/MIT-6-824-Paxos/tree/main/src>

