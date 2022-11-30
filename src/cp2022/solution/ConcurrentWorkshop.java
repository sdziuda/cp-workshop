package cp2022.solution;

import cp2022.base.Workplace;
import cp2022.base.WorkplaceId;
import cp2022.base.Workshop;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public class ConcurrentWorkshop implements Workshop {

    private final List<Workplace> workplaces;
    private final Map<WorkplaceId, Long> workplaceOwners;
    private final Map<WorkplaceId, Queue<Thread>> queueToWorkplace;
    private final Semaphore mutex;
    private final Semaphore[] workplaceSemaphores;

    public ConcurrentWorkshop(Collection<Workplace> workplaces) {
        this.workplaces = List.copyOf(workplaces);
        this.workplaceOwners = new ConcurrentHashMap<>();
        for (Workplace workplace : workplaces) {
            workplaceOwners.put(workplace.getId(), 0L);
        }
        this.queueToWorkplace = new ConcurrentHashMap<>();
        for (Workplace workplace : workplaces) {
            queueToWorkplace.put(workplace.getId(), new ConcurrentLinkedQueue<>(Collections.emptyList()));
        }
        this.mutex = new Semaphore(1, true);
        workplaceSemaphores = new Semaphore[workplaces.size()];
        for (int i = 0; i < workplaces.size(); i++) {
            workplaceSemaphores[i] = new Semaphore(1, true);
        }
    }

    @Override
    public Workplace enter(WorkplaceId wid) {
        try {
            mutex.acquire();
            queueToWorkplace.get(wid).add(Thread.currentThread());
            mutex.release();

            return this.enterIfFirstAndEmpty(wid);
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    @Override
    public Workplace switchTo(WorkplaceId wid) {
        if (workplaceOwners.get(wid).equals(Thread.currentThread().getId())) {
            return this.getWorkplace(wid);
        } else {
            try {
                mutex.acquire();
                WorkplaceId oldWid = this.getWorkplaceId(Thread.currentThread());
                workplaceOwners.put(oldWid, 0L);
                workplaceSemaphores[this.getWorkplaceIndex(oldWid)].release();
                queueToWorkplace.get(wid).add(Thread.currentThread());
                mutex.release();

                return this.enterIfFirstAndEmpty(wid);
            } catch (InterruptedException e) {
                throw new RuntimeException("panic: unexpected thread interruption");
            }
        }
    }

    @Override
    public void leave() {
        try {
            mutex.acquire();
            WorkplaceId wid = this.getWorkplaceId(Thread.currentThread());
            workplaceOwners.put(wid, 0L);
            workplaceSemaphores[this.getWorkplaceIndex(wid)].release();
            mutex.release();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    private WorkplaceId getWorkplaceId(Thread thread) {
        return Objects.requireNonNull(workplaceOwners.entrySet().stream()
                .filter(e -> e.getValue().equals(thread.getId())).findFirst().orElse(null)).getKey();
    }

    private int getWorkplaceIndex(WorkplaceId wid) {
        return workplaces.indexOf(workplaces.stream().filter(w -> w.getId().equals(wid)).findFirst().orElse(null));
    }

    private Workplace getWorkplace(WorkplaceId wid) {
        return workplaces.stream().filter(w -> w.getId().equals(wid)).findFirst().orElse(null);
    }

    private Workplace enterIfFirstAndEmpty(WorkplaceId wid) {
        try {
            Workplace workplace = this.getWorkplace(wid);
            while (true) {
                mutex.acquire();
                if (workplaceOwners.get(wid) == 0L && queueToWorkplace.get(wid).peek() == Thread.currentThread()) {
                    workplaceSemaphores[workplaces.indexOf(workplace)].acquire();
                    workplaceOwners.put(wid, Thread.currentThread().getId());
                    queueToWorkplace.get(wid).poll();
                    mutex.release();
                    return workplace;
                } else {
                    mutex.release();
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }
}
