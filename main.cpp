#include "coroutine_io.hpp"

int main()
{
    HookSystem::initialize();
    auto& scheduler = Scheduler::instance();

    std::cout << "=== 演示 1：多协程调度 ===" << std::endl;

    static int args[] = {1, 2, 3};

    for (int& i : args)
    {
        scheduler.create_coroutine([](void* arg)
        {
            const auto id = static_cast<int*>(arg);
            printf("协程 %d 开始\n", *id);

            for (int j = 0; j < 3; j++)
            {
                printf("协程 %d 工作 %d\n", *id, j + 1);
                Scheduler::instance().yield();
            }

            printf("协程 %d 结束\n", *id);
        }, &i);
    }

    std::cout << std::endl << "=== 演示 2：异步 I/O ===" << std::endl;

    int sockpair[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, sockpair);

    scheduler.create_coroutine([](void* arg)
    {
        const auto read_fd = static_cast<int*>(arg);
        char buffer[64];

        printf("协程准备读取socket %d\n", *read_fd);

        if (const ssize_t n = read(*read_fd, buffer, sizeof(buffer)); n > 0)
        {
            buffer[n] = '\0';
            printf("协程读取到: %s\n", buffer);
        }
    }, &sockpair[0]);

    scheduler.create_coroutine([](void* arg)
    {
        const auto write_fd = static_cast<int*>(arg);

        for (int i = 0; i < 3; i++)
        {
            Scheduler::instance().yield();
        }

        printf("协程写入数据到socket %d\n", *write_fd);
        write(*write_fd, "Hello from coroutine!", 21);
        close(*write_fd);
    }, &sockpair[1]);

    scheduler.run();

    close(sockpair[0]);
    return 0;
}
