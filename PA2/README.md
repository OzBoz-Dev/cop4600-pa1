# PA 2
Ozair Ahmed and Ashar Iqbal

AI Disclosure:

Models used: Github Copilot, Devin (Fusion mode: Claude Opus 5.5 and Devin SWE-2)

Prompts:

Github Copilot (11/19/2026)
```
> Help me understand this assignment description and what exactly it's asking me to do
#attachment:Pasted text #1

> Create an instructions.md file with instructions for us to implement it
```

These were used to parse the assignment instructions and understand the problem better, as well as the details that would make for a successful implementation.

Devin CLI (11/20/2026)
```
> Implement mapreduce.c according to the instructions.md file

> Explain the code to me, function by function

> Explain the testing methodology
```
This was one of my first times using CLI coding agents on such a large scale (a "one-shot" prompt, if you will). I was using Devin CLI, which contains both its own models as well as frontier models like Claude Opus 5.5.

I had the agent write the code almost entirely on its own, as well as test its own work. Because this means running tool calls both to read files, write to files, and install packages and write in /tmp, I used a tool called **Docker Sandboxes** to isolate it. This creates a dedicated MicroVM fully isolated from my main system, with its own docker engine, and bind mount access only to the project directory. It also restricts network access from the sandbox. This allows for me to test the capabilities of coding agents without worrying about unauthorized access to my files, network or shell, and allows for running sudo commands within the sandbox without any system concerns.

After I ran the first prompt, the agent proceeded to read the instructions file from before and write the 368 lines of `mapreduce.c` in one go.

It then prompted me for how it should test its work, and I instructed it to write a temp file for testing, and install gcc to build and test, and valgrind for memory stress tests in its environment. It created its own shell script for testing different test cases, and ran them against the program to confirm proper behavior. Once all tests passed, it gave me a brief confirmation.

One thing to watch out for with these kinds of agents is ensuring that you understand the code being written. When an agent spits out hundreds of lines of code, you have to be sure that the code is high quality and does the job.

Thus, I also instructed the agent to explain the code, function by function, as well as its testing methodology. It explained the main data structures: KeyEntry, Partition, Job, ThreadArgs, active_job and active_lock. Also explained helper functions, and the steps of the mapping phase, and the reducing phase.

Finally, it explained the testing methodology, the command used to compile `gcc -Wall -Werror -pthread -O -c mapreduce.c`, the temporary test program, and the input files, including edge cases such as an empty file and a file with 100,000 lines. It also showed how it used valgrind to check for memory leaks, and helgrind to check for race conditions, with both coming back clean. Thus, I was finally confident enough in my understanding of the code.