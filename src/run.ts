import { IExecutor } from './Executor';
import ITask from './Task';

class Semaphore {
    private max: number;
    private current: number = 0;
    private waiting: Array<() => void> = [];
  
    constructor(max: number) {
      this.max = max;
    }
  
    acquire(): Promise<() => void> {
      // Если ограничений нет, сразу возвращаем dummy-функцию
      if (this.max === 0) {
        return Promise.resolve(() => {});
      }
      if (this.current < this.max) {
        this.current++;
        return Promise.resolve(() => {
          this.current--;
          if (this.waiting.length > 0) {
            const next = this.waiting.shift();
            if (next) next();
          }
        });
      }
      return new Promise(resolve => {
        this.waiting.push(() => {
          this.current++;
          resolve(() => {
            this.current--;
            if (this.waiting.length > 0) {
              const next = this.waiting.shift();
              if (next) next();
            }
          });
        });
      });
    }
  }

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0) {
    maxThreads = Math.max(0, maxThreads);
     // Создаём семафор для контроля количества одновременно выполняемых задач
  const semaphore = new Semaphore(maxThreads);

  // Для каждого targetId поддерживаем цепочку (promise chain) для последовательного выполнения
  const targetChains = new Map<number, Promise<void>>();
  // Массив для отслеживания всех запланированных задач
  const allTasks: Promise<void>[] = [];

  // Обработка очереди задач
  for await (const task of queue) {
    // Берём текущую цепочку для данного targetId или начинаем с resolved Promise
    const currentChain = targetChains.get(task.targetId) || Promise.resolve();

    // Добавляем задачу в цепочку, чтобы обеспечить последовательность
    const taskPromise = currentChain.then(async () => {
      // Ожидаем разрешения семафора, если ограничение задано
      const release = await semaphore.acquire();
      try {
        await executor.executeTask(task);
      } finally {
        // Освобождаем семафор после выполнения задачи
        release();
      }
    });

    // Обновляем цепочку для данного targetId
    targetChains.set(task.targetId, taskPromise);
    // Добавляем задачу в общий список для последующего ожидания завершения
    allTasks.push(taskPromise);
  }

  // Дожидаемся завершения всех запланированных задач
  await Promise.all(allTasks);

  // Возвращаем пустой объект (можно расширить отчётом, если требуется)
  return {};
}
