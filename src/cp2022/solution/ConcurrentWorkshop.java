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
    private final Queue<Long> workshopQueue;
    private int howManyThreadsAreInWorkshop;
    private int howManyEnteredNow;

    public ConcurrentWorkshop(Collection<Workplace> workplaces) {
        this.workplaces = new ArrayList<>();
        for (Workplace workplace : workplaces) {
            this.workplaces.add(new WorkplaceWrapper(workplace, this));
        }
        this.threadSemaphores = new ConcurrentHashMap<>();
        this.mutex = new Semaphore(1, true);
        this.whereToSwitch = new int[this.workplaces.size()];
        Arrays.fill(this.whereToSwitch, -1);
        this.workshopQueue = new ConcurrentLinkedQueue<>();
        this.howManyThreadsAreInWorkshop = 0;
        this.howManyEnteredNow = 0;
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
                .orElse(null);
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
        private Queue<WorkplaceId> line;
        private CountDownLatch latch;
        private CountDownLatch lineLatch;

        public WorkplaceWrapper(Workplace workplace, ConcurrentWorkshop workshop) {
            super(workplace.getId());
            this.workplace = workplace;
            this.workshop = workshop;
            this.owner = -1;
            this.queue = new ConcurrentLinkedQueue<>();
            this.cycle = null;
            this.line = null;
        }

        public void enter() {
            try {
                this.workshop.mutex.acquire();
                if (this.workshop.howManyEnteredNow >= 2 * this.workshop.workplaces.size()) {
                    this.workshop.workshopQueue.add(Thread.currentThread().getId());
                    this.workshop.mutex.release();
                    this.workshop.threadSemaphores.get(Thread.currentThread().getId()).acquire();
                    this.workshop.workshopQueue.poll();
                }
                this.workshop.howManyThreadsAreInWorkshop++;
                this.workshop.howManyEnteredNow++;
                if (this.owner != -1) {
                    this.queue.add(Thread.currentThread().getId());
                    if (this.workshop.howManyEnteredNow == 2 * this.workshop.workplaces.size() ||
                            this.workshop.workshopQueue.isEmpty()) {
                        this.workshop.mutex.release();
                    } else {
                        this.workshop.threadSemaphores.get(this.workshop.workshopQueue.peek()).release();
                    }
                    this.workshop.threadSemaphores.get(Thread.currentThread().getId()).acquire();
                    this.queue.remove(Thread.currentThread().getId());
                }
                var previousOwner = this.owner;
                this.owner = Thread.currentThread().getId();
                if (this.line != null) {
                    this.workshop.threadSemaphores.get(previousOwner).release();
                } else if (this.workshop.howManyEnteredNow == 2 * this.workshop.workplaces.size() ||
                        this.workshop.workshopQueue.isEmpty()) {
                    this.workshop.mutex.release();
                } else {
                    this.workshop.threadSemaphores.get(this.workshop.workshopQueue.peek()).release();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("panic: unexpected thread interruption");
            }
        }

        public void switchTo(WorkplaceWrapper workplaceTo) {
            try {
                if (workplaceTo.getId() == this.getId()) {
                    this.workshop.mutex.release();
                } else if (workplaceTo.owner == -1) {
                    if (!this.queue.isEmpty()) {
                        long prev = this.queue.peek();
                        Queue<WorkplaceId> actLine = new ConcurrentLinkedQueue<>();
                        actLine.add(workplaceTo.getId());
                        actLine.add(this.getId());
                        WorkplaceWrapper prevWorkplace = this;
                        while (!prevWorkplace.queue.isEmpty()) {
                            prev = prevWorkplace.queue.peek();
                            prevWorkplace = this.workshop.getWorkplaceWrapper(prev);
                            if (prevWorkplace == null || prevWorkplace.queue.isEmpty()) {
                                break;
                            } else {
                                actLine.add(prevWorkplace.getId());
                            }
                        }
                        Queue<WorkplaceId> lineCopy = new ConcurrentLinkedQueue<>(actLine);
                        int lineSize = lineCopy.size();
                        while (!actLine.isEmpty()) {
                            WorkplaceWrapper workplace = this.workshop.getWorkplaceWrapper(actLine.poll());
                            workplace.line = lineCopy;
                            workplace.lineLatch = new CountDownLatch(lineSize);
                        }
                        this.workshop.threadSemaphores.get(prev).release();
                        this.workshop.threadSemaphores.get(Thread.currentThread().getId()).acquire();
                        workplaceTo.owner = Thread.currentThread().getId();
                    } else {
                        workplaceTo.owner = Thread.currentThread().getId();
                        this.owner = -1;
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
                        } else if (workplaceTo.line != null){
                            this.workshop.threadSemaphores.get(oldWorkplaceToOwner).release();
                        } else {
                            this.owner = -1;
                            this.workshop.mutex.release();
                        }
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("panic: unexpected thread interruption");
            }
        }

        public void leave() {
            this.owner = -1;
            this.workshop.howManyThreadsAreInWorkshop--;
            if (this.workshop.howManyThreadsAreInWorkshop == 0) {
                this.workshop.howManyEnteredNow = 0;
                if (!this.workshop.workshopQueue.isEmpty()) {
                    this.workshop.threadSemaphores.get(this.workshop.workshopQueue.peek()).release();
                } else {
                    this.workshop.mutex.release();
                }
            } else if (!this.queue.isEmpty()) {
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
            } else if (this.line != null) {
                try {
                    for (WorkplaceId id : line) {
                        WorkplaceWrapper workplace = this.workshop.getWorkplaceWrapper(id);
                        var latch = workplace.lineLatch;
                        latch.countDown();
                    }
                    WorkplaceId lineStart = this.line.peek();
                    this.lineLatch.await();
                    this.line = null;
                    if (lineStart == this.getId()) {
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
