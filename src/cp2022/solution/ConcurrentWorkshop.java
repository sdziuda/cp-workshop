package cp2022.solution;

import cp2022.base.Workplace;
import cp2022.base.WorkplaceId;
import cp2022.base.Workshop;

import java.util.*;
import java.util.concurrent.*;

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
        this.mutex = new Semaphore(1, true);
        this.whereToSwitch = new int[this.workplaces.size()];
        Arrays.fill(this.whereToSwitch, -1);
    }

    @Override
    public WorkplaceWrapper enter(WorkplaceId wid) {
        this.threadSemaphores.putIfAbsent(Thread.currentThread().getId(), new Semaphore(0));
        WorkplaceWrapper workplaceWrapper = this.getWorkplaceWrapper(wid);
        workplaceWrapper.enter();
        return workplaceWrapper;
    }

    @Override
    public WorkplaceWrapper switchTo(WorkplaceId wid) {
        WorkplaceWrapper workplaceTo = this.getWorkplaceWrapper(wid);
        try {
            this.mutex.acquire();
            WorkplaceWrapper workplaceFrom = this.getWorkplaceWrapper(Thread.currentThread().getId());
            workplaceFrom.switchTo(workplaceTo);
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
        return workplaceTo;
    }

    @Override
    public void leave() {
        try {
            this.mutex.acquire();
            WorkplaceWrapper workplaceWrapper = this.getWorkplaceWrapper(Thread.currentThread().getId());
            workplaceWrapper.leave();
            this.threadSemaphores.remove(Thread.currentThread().getId());
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    private WorkplaceWrapper getWorkplaceWrapper(WorkplaceId wid) {
        return this.workplaces.stream()
                .filter(workplaceWrapper -> workplaceWrapper.getId() == wid)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("panic: workplace not found"));
    }

    private WorkplaceWrapper getWorkplaceWrapper(long threadId) {
        return this.workplaces.stream()
                .filter(workplaceWrapper -> workplaceWrapper.owner == threadId)
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
        private Queue<WorkplaceId> cycle;
        private CountDownLatch latch;

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
                if (workplaceTo.getId() == this.getId()) {
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
                    boolean cycle = false;
                    while (this.workshop.whereToSwitch[indexTo] != -1) {
                        if (this.workshop.whereToSwitch[indexTo] == index) {
                            cycle = true;
                            break;
                        }
                        indexTo = this.workshop.whereToSwitch[indexTo];
                    }
                    if (cycle) {
                        indexTo = this.workshop.whereToSwitch[index];
                        var queueCycle = new ConcurrentLinkedDeque<WorkplaceId>();
                        queueCycle.add(this.getId());
                        while (indexTo != index) {
                            queueCycle.add(this.workshop.workplaces.get(indexTo).getId());
                            indexTo = this.workshop.whereToSwitch[indexTo];
                        }
                        Queue<WorkplaceId> cycleCopy = new ConcurrentLinkedQueue<>(queueCycle);
                        int size = queueCycle.size();
                        while (!queueCycle.isEmpty()) {
                            WorkplaceId id = queueCycle.pollLast();
                            WorkplaceWrapper workplace = this.workshop.getWorkplaceWrapper(id);
                            workplace.cycle = cycleCopy;
                            workplace.latch = new CountDownLatch(size);
                        }
                        this.workshop.threadSemaphores.get(workplaceTo.owner).release();
                        this.workshop.threadSemaphores.get(Thread.currentThread().getId()).acquire();
                        this.workshop.whereToSwitch[index] = -1;
                        workplaceTo.owner = Thread.currentThread().getId();
                    } else {
                        workplaceTo.queue.add(Thread.currentThread().getId());
                        this.workshop.mutex.release();
                        this.workshop.threadSemaphores.get(Thread.currentThread().getId()).acquire();
                        workplaceTo.queue.remove(Thread.currentThread().getId());
                        this.workshop.whereToSwitch[index] = -1;
                        long oldWorkplaceToOwner = workplaceTo.owner;
                        workplaceTo.owner = Thread.currentThread().getId();
                        if (this.cycle != null) {
                            this.workshop.threadSemaphores.get(oldWorkplaceToOwner).release();
                        } else {
                            this.workshop.whereToSwitch[this.workshop.getWorkplaceIndex(this)] = -1;
                            if (!this.queue.isEmpty()) {
                                this.workshop.threadSemaphores.get(this.queue.peek()).release();
                            } else {
                                this.owner = -1;
                                this.workshop.mutex.release();
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("panic: unexpected thread interruption");
            }
        }

        public void leave() {
            this.owner = -1;
            if (!this.queue.isEmpty()) {
                this.workshop.threadSemaphores.get(this.queue.peek()).release();
            } else {
                this.workshop.mutex.release();
            }
        }

        @Override
        public void use() {
            if (this.cycle != null) {
                try {
                    for (WorkplaceId id : cycle) {
                        WorkplaceWrapper workplace = this.workshop.getWorkplaceWrapper(id);
                        var latch = workplace.latch;
                        latch.countDown();
                    }
                    WorkplaceId cycleStart = this.cycle.peek();
                    this.latch.await();
                    this.cycle = null;
                    if (cycleStart == this.getId()) {
                        this.workshop.mutex.release();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException("panic: unexpected thread interruption");
                }
            }

            this.workplace.use();
        }
    }
}
