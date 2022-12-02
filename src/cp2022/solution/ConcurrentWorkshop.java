package cp2022.solution;

import cp2022.base.Workplace;
import cp2022.base.WorkplaceId;
import cp2022.base.Workshop;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public class ConcurrentWorkshop implements Workshop {

    private final List<WorkplaceWrapper> workplaces;
    private final Map<Long, Semaphore> threadSemaphores;

    public ConcurrentWorkshop(Collection<Workplace> workplaces) {
        this.workplaces = new ArrayList<>();
        for (Workplace workplace : workplaces) {
            this.workplaces.add(new WorkplaceWrapper(workplace));
        }
        this.threadSemaphores = new ConcurrentHashMap<>();
    }

    @Override
    public Workplace enter(WorkplaceId wid) {
        this.threadSemaphores.putIfAbsent(Thread.currentThread().getId(), new Semaphore(0, true));
        WorkplaceWrapper workplaceWrapper = this.getWorkplaceWrapper(wid);
        workplaceWrapper.enter(this.threadSemaphores);
        return workplaceWrapper.getWorkplace();
    }

    @Override
    public Workplace switchTo(WorkplaceId wid) {
        WorkplaceWrapper workplaceFrom = this.getWorkplaceWrapper(Thread.currentThread());
        WorkplaceWrapper workplaceTo = this.getWorkplaceWrapper(wid);
        workplaceFrom.leave(this.threadSemaphores);
        workplaceTo.enter(this.threadSemaphores);
        return workplaceTo.getWorkplace();
    }

    @Override
    public void leave() {
        WorkplaceWrapper workplaceWrapper = this.getWorkplaceWrapper(Thread.currentThread());
        workplaceWrapper.leave(this.threadSemaphores);
        threadSemaphores.remove(Thread.currentThread().getId());
    }

    private WorkplaceWrapper getWorkplaceWrapper(WorkplaceId wid) {
        return this.workplaces.stream()
                .filter(workplaceWrapper -> workplaceWrapper.getWorkplace().getId() == wid)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("panic: workplace not found"));
    }

    private WorkplaceWrapper getWorkplaceWrapper(Thread t) {
        return this.workplaces.stream()
                .filter(workplaceWrapper -> workplaceWrapper.getOwner() == t.getId())
                .findFirst()
                .orElseThrow(() -> new RuntimeException("panic: workplace not found"));
    }

    private static class WorkplaceWrapper {
        private final Workplace workplace;
        private long owner;
        private final Queue<Long> queue;
        private final Semaphore workplaceMutex;

        public WorkplaceWrapper(Workplace workplace) {
            this.workplace = workplace;
            this.owner = -1;
            this.queue = new ConcurrentLinkedQueue<>();
            this.workplaceMutex = new Semaphore(1, true);
        }

        public Workplace getWorkplace() {
            return this.workplace;
        }

        public long getOwner() {
            return this.owner;
        }

        public void enter(Map<Long, Semaphore> threadSemaphores) {
            try {
                this.workplaceMutex.acquire();
                if (this.owner != -1) {
                    this.queue.add(Thread.currentThread().getId());
                    this.workplaceMutex.release();
                    threadSemaphores.get(Thread.currentThread().getId()).acquire();
                    this.queue.poll();
                }
                this.owner = Thread.currentThread().getId();
            } catch (InterruptedException e) {
                throw new RuntimeException("panic: unexpected thread interruption");
            } finally {
                this.workplaceMutex.release();
            }
        }

        public void leave(Map<Long, Semaphore> threadSemaphores) {
            try {
                this.workplaceMutex.acquire();
                this.owner = -1;
                if (!this.queue.isEmpty()) {
                    threadSemaphores.get(this.queue.peek()).release();
                } else {
                    this.workplaceMutex.release();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("panic: unexpected thread interruption");
            }
        }
    }
}
