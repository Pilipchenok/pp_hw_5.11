#include <random>
#include <thread>
#include <mutex>
#include <vector>
#include <queue>
#include <condition_variable>
#include <functional>
#include <iostream>

// REALIZED TASKS 2 & 3

struct task_t {
  void operator()();
};

static std::mutex cout_mx;

void task_t::operator()() {
  thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution< int > dist(100, 3000);

  std::this_thread::sleep_for(std::chrono::milliseconds(dist(gen)));
  std::lock_guard< std::mutex > lock(cout_mx); //osyncstream нет на macOS :(((
  std::cout << "!\n";
}

struct th_pool_t
{
    using f_t = std::function< void(void) >;
    explicit th_pool_t(unsigned int k);
    ~th_pool_t();
    void submit(f_t f);
    void wait();
    void pause();
    void resume();
  private:
    void execute(size_t id);
    std::queue< f_t > tasks_;
    size_t count_tasks_ = 0;
    std::condition_variable cw_;
    std::mutex mx_;
    std::condition_variable cd_;
    bool stop_{false};
    bool paused_{false};
    std::vector< std::thread > ths_;
    size_t threadsCount_ = 0;
    std::vector < std::queue< f_t > > db_;
    std::vector<std::mutex> db_mxs_;
};

void th_pool_t::submit(f_t f)
{
  mx_.lock();
  tasks_.push(std::move(f));
  count_tasks_++;
  mx_.unlock();
  cd_.notify_one();
}

th_pool_t::th_pool_t(unsigned int k):
  threadsCount_(k),
  db_(k),
  db_mxs_(k)
{
  auto thf = [this](size_t id) { execute(id); };
  for (size_t i = 0; i < k; ++i)
  {
    ths_.emplace_back(thf, i);
  }
}

th_pool_t::~th_pool_t()
{
  mx_.lock();
  stop_ = true;
  paused_ = false;
  mx_.unlock();
  cd_.notify_all();
  for (auto && th: ths_) th.join();
}

void th_pool_t::execute(size_t id) {
  std::unique_lock<std::mutex> lck(mx_);
  while (!stop_ || !tasks_.empty()) {
    cd_.wait(lck, [&]() {
      return (!tasks_.empty() && !paused_) || stop_;
    });

    if (tasks_.empty()) {
      continue;
    }

    size_t amount_of_taking = std::max<size_t>(1, tasks_.size() / threadsCount_);
    {
      std::lock_guard<std::mutex> db_lock(db_mxs_[id]);
      for (size_t i = 0; i < amount_of_taking && !tasks_.empty(); ++i) {
        db_[id].push(std::move(tasks_.front()));
        tasks_.pop();
      }
    }

    lck.unlock();

    size_t completed = 0;
    while (true) {
      f_t task;
      bool has_task = false;
      {
        std::lock_guard<std::mutex> db_lock(db_mxs_[id]);
        if (!db_[id].empty())
        {
          task = std::move(db_[id].front());
          db_[id].pop();
          has_task = true;
        }
      }

      if (!has_task)
      {
        for (size_t i = 0; i < threadsCount_; ++i) {
          if (i == id)
          {
            continue;
          }
          db_mxs_[i].lock();
          if (!db_[i].empty())
          {
            task = std::move(db_[i].front());
            db_[i].pop();
            db_mxs_[i].unlock();
            has_task = true;
            break;
          }
          db_mxs_[i].unlock();
        }
      }

      if (!has_task)
      {
        break;
      }

      try
      {
        task();
      } catch (...) {}
      
      completed++;
    }

    lck.lock();
    count_tasks_ -= completed;
    if (count_tasks_ == 0) {
      cw_.notify_all();
    }
  }
}

void th_pool_t::wait()
{
  std::unique_lock< std::mutex > lck(mx_);
  auto proceed = [&]() {
    return count_tasks_ == 0;
  };
  cw_.wait(lck, proceed);
}

void th_pool_t::pause()
{
  mx_.lock();
  paused_ = true;
  mx_.unlock();
}

void th_pool_t::resume()
{
  mx_.lock();
  paused_ = false;
  mx_.unlock();
  cd_.notify_all();
}

int main()
{
  th_pool_t pool(5);
  for (size_t i = 0; i < 30; ++i)
  {
    pool.submit(task_t{});
    if (i == 5)
    {
      pool.wait();
      pool.pause();
      std::cout << "paused\n";
      std::this_thread::sleep_for(std::chrono::seconds(1));
      pool.resume();
      std::cout << "resumed\n";
    }
  }
  pool.wait();
  std::cout << "123\n";
}
