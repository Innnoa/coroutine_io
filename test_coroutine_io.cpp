#include "coroutine_io.hpp"
#include <cassert>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) \
    do { \
        std::cout << "  TEST: " << name << " ... "; \
    } while(0)

#define PASS() \
    do { \
        std::cout << "PASSED" << std::endl; \
        tests_passed++; \
    } while(0)

#define FAIL(msg) \
    do { \
        std::cout << "FAILED: " << msg << std::endl; \
        tests_failed++; \
    } while(0)

#define ASSERT(cond, msg) \
    do { \
        if (!(cond)) { FAIL(msg); return; } \
    } while(0)

void test_coroutine_creation_and_switch()
{
    TEST("协程创建与切换");

    static int value = 0;

    auto& s = Scheduler::instance();
    s.create_coroutine([](void* arg) {
        auto* val = static_cast<int*>(arg);
        *val = 42;
    }, &value);

    s.run();
    ASSERT(value == 42, "协程未正确执行");

    value = 0;

    s.restart();
    s.create_coroutine([](void* arg) {
        auto* val = static_cast<int*>(arg);
        *val = 10;
        Scheduler::instance().yield();
        *val = 20;
        Scheduler::instance().yield();
        *val = 30;
    }, &value);

    s.run();
    ASSERT(value == 30, "协程 yield 后未正确恢复执行");

    PASS();
}

void test_epoll_hook()
{
    TEST("epoll 钩子与异步 I/O");

    int sp[2];
    ASSERT(socketpair(AF_UNIX, SOCK_STREAM, 0, sp) == 0, "socketpair 创建失败");

    static char recv_buf[64] = {};
    static bool read_done = false;

    auto& s = Scheduler::instance();
    s.restart();

    s.create_coroutine([](void* arg) {
        auto* fd = static_cast<int*>(arg);
        if (const ssize_t n = read(*fd, recv_buf, sizeof(recv_buf)); n > 0) {
            recv_buf[n] = '\0';
            read_done = true;
        }
    }, &sp[0]);

    static const char* msg = "coroutine_io_test";
    s.create_coroutine([](void* arg) {
        auto* fd = static_cast<int*>(arg);
        Scheduler::instance().yield();
        write(*fd, msg, strlen(msg));
        close(*fd);
    }, &sp[1]);

    s.run();

    close(sp[0]);

    ASSERT(read_done, "异步读取未完成");
    ASSERT(strcmp(recv_buf, msg) == 0, "读取内容不匹配");

    PASS();
}

void test_scheduler_round_robin()
{
    TEST("调度器 Round-Robin 调度");

    static int count = 0;
    static int seq[6] = {};
    static int idx = 0;

    auto& s = Scheduler::instance();
    s.restart();

    static int ids[] = {0, 1, 2};

    for (int& id_val : ids) {
        s.create_coroutine([](void* arg) {
            auto id = static_cast<int*>(arg);
            seq[idx++] = *id * 10 + 1;
            count++;
            Scheduler::instance().yield();
            seq[idx++] = *id * 10 + 2;
            count++;
        }, &id_val);
    }

    s.run();

    ASSERT(count == 6, "协程调度次数不正确");
    ASSERT(seq[0] == 1 && seq[1] == 11 && seq[2] == 21, "第一轮调度顺序错误");
    ASSERT(seq[3] == 2 && seq[4] == 12 && seq[5] == 22, "第二轮调度顺序错误");

    PASS();
}

int main()
{
    HookSystem::initialize();

    std::cout << "=== coroutine_io 测试套件 ===" << std::endl << std::endl;

    test_coroutine_creation_and_switch();
    test_epoll_hook();
    test_scheduler_round_robin();

    std::cout << std::endl;
    std::cout << "结果: " << tests_passed << " 通过, "
              << tests_failed << " 失败" << std::endl;

    return tests_failed > 0 ? 1 : 0;
}
