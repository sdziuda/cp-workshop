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
    private final Semaphore mutex;
    private final int[] whereToSwitch;

    public ConcurrentWorkshop(Collection<Workplace> workplaces) {
        this.workplaces = new ArrayList<>();
        for (Workplace workplace : workplaces) {
            this.workplaces.add(new WorkplaceWrapper(workplace, this));
        }
        this.threadSemaphores = new ConcurrentHashMap<>();
        this.mutex = new Semaphore(1);
        this.whereToSwitch = new int[this.workplaces.size()];
        Arrays.fill(this.whereToSwitch, -1);
    }

    @Override
    public Workplace enter(WorkplaceId wid) {
        this.threadSemaphores.putIfAbsent(Thread.currentThread().getId(), new Semaphore(0));
        WorkplaceWrapper workplaceWrapper = this.getWorkplaceWrapper(wid);
        workplaceWrapper.enter();
        return workplaceWrapper;
    }

    @Override
    public Workplace switchTo(WorkplaceId wid) {
        WorkplaceWrapper workplaceFrom = this.getWorkplaceWrapper(Thread.currentThread());
        WorkplaceWrapper workplaceTo = this.getWorkplaceWrapper(wid);
        workplaceFrom.switchTo(workplaceTo);
        return workplaceTo;
    }

    @Override
    public void leave() {
        WorkplaceWrapper workplaceWrapper = this.getWorkplaceWrapper(Thread.currentThread());
        workplaceWrapper.leave();
        threadSemaphores.remove(Thread.currentThread().getId());
    }

    private WorkplaceWrapper getWorkplaceWrapper(WorkplaceId wid) {
        return this.workplaces.stream()
                .filter(workplaceWrapper -> workplaceWrapper.getId() == wid)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("panic: workplace not found"));
    }

    private WorkplaceWrapper getWorkplaceWrapper(Thread t) {
        return this.workplaces.stream()
                .filter(workplaceWrapper -> workplaceWrapper.owner == t.getId())
                .findFirst()
                .orElseThrow(() -> new RuntimeException("panic: workplace not found"));
    }

    private int getWorkplaceIndex(WorkplaceWrapper workplace) {
        return this.workplaces.indexOf(workplace);
    }

    private static class WorkplaceWrapper extends Workplace {
        private final Workplace workplace;
        private final ConcurrentWorkshop workshop;
        private long owner;
        private final Queue<Long> queue;

        public WorkplaceWrapper(Workplace workplace, ConcurrentWorkshop workshop) {
            super(workplace.getId());
            this.workplace = workplace;
            this.workshop = workshop;
            this.owner = -1;
            this.queue = new ConcurrentLinkedQueue<>();
        }

        public void enter() {
            try {
                this.workshop.mutex.acquire();
                if (this.owner != -1) {
                    this.queue.add(Thread.currentThread().getId());
                    this.workshop.mutex.release();
                    this.workshop.threadSemaphores.get(Thread.currentThread().getId()).acquire();
                    this.queue.poll();
                }
                this.owner = Thread.currentThread().getId();
                this.workshop.mutex.release();
            } catch (InterruptedException e) {
                throw new RuntimeException("panic: unexpected thread interruption");
            }
        }

        public void switchTo(WorkplaceWrapper workplaceTo) {
            try {
                this.workshop.mutex.acquire();
                if (workplaceTo.getId() == this.getId()) {
                    this.owner = -1;
                    this.queue.add(Thread.currentThread().getId());
                    this.queue.poll();
                    this.owner = Thread.currentThread().getId();
                    this.workshop.threadSemaphores.get(this.owner).release();
                    this.workshop.mutex.release();
                } else if (workplaceTo.owner == -1) {
                    workplaceTo.owner = Thread.currentThread().getId();
                    this.owner = -1;
                    if (!this.queue.isEmpty()) {
                        this.workshop.threadSemaphores.get(this.queue.peek()).release();
                    } else {
                        this.workshop.mutex.release();
                    }
                } else {
                    int index = this.workshop.getWorkplaceIndex(this);
                    int indexTo = this.workshop.getWorkplaceIndex(workplaceTo);
                    this.workshop.whereToSwitch[index] = indexTo;
                    workplaceTo.queue.add(Thread.currentThread().getId());
                    // cycle detection
                    this.workshop.mutex.release();
                    this.workshop.threadSemaphores.get(Thread.currentThread().getId()).acquire();
                    this.workshop.whereToSwitch[index] = -1;
                    this.owner = -1;
                    workplaceTo.queue.poll();
                    workplaceTo.owner = Thread.currentThread().getId();
                    this.workshop.mutex.release();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("panic: unexpected thread interruption");
            }
        }

        public void leave() {
            try {
                this.workshop.mutex.acquire();
                this.owner = -1;
                if (!this.queue.isEmpty()) {
                    this.workshop.threadSemaphores.get(this.queue.peek()).release();
                } else {
                    this.workshop.mutex.release();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("panic: unexpected thread interruption");
            }
        }

        @Override
        public void use() {
            this.workplace.use();
        }
    }
}
