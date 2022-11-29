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
            Workplace workplace = workplaces.stream().filter(w -> w.getId().equals(wid)).findFirst().orElse(null);
            queueToWorkplace.get(wid).add(Thread.currentThread());
            mutex.release();
            workplaceSemaphores[workplaces.indexOf(workplace)].acquire();
            mutex.acquire();
            workplaceOwners.put(wid, Thread.currentThread().getId());
            queueToWorkplace.get(wid).remove(Thread.currentThread());
            mutex.release();
            return workplace;
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    @Override
    public Workplace switchTo(WorkplaceId wid) {
        leave();
        return enter(wid);
    }

    @Override
    public void leave() {
        try {
            mutex.acquire();
            WorkplaceId wid = Objects.requireNonNull(workplaceOwners.entrySet().stream()
                    .filter(e -> e.getValue().equals(Thread.currentThread().getId())).findFirst().orElse(null)).getKey();
            workplaceOwners.put(wid, 0L);
            mutex.release();
            workplaceSemaphores[workplaces.indexOf(workplaces.stream().filter(w -> w.getId().equals(wid))
                    .findFirst().orElse(null))].release();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }
}
