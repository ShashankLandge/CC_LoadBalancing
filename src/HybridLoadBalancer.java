import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;    // <-- pull in IntStream, Collectors, etc.

public class HybridLoadBalancer {
    // Configuration constants
    private static final double MIPS_INCREASE_FACTOR = 1.2;
    private static final int MAX_MIPS = 2500;
    private static final double LOAD_IMBALANCE_THRESHOLD = 0.2;
    // private static final double SLA_VIOLATION_CHECK_THRESHOLD = 1.0;
    private static final double SLA_VIOLATION_CHECK_THRESHOLD = 0.0;
    
    private final List<VirtualMachine> virtualMachines;
    private final Queue<Task> globalQueue;
    private final Map<VirtualMachine, Queue<Task>> localQueues;
    private final ACOLoadBalancer acoLoadBalancer;
    private final HLBZIDLoadBalancer hlbzidLoadBalancer;
    private int slaViolationsCount;
    
    public HybridLoadBalancer(int vmCount) {
        this.virtualMachines = new CopyOnWriteArrayList<>();
        this.globalQueue = new ConcurrentLinkedQueue<>();
        this.localQueues = new ConcurrentHashMap<>();
        this.slaViolationsCount = 0;

        initializeVirtualMachines(vmCount);
        
        this.acoLoadBalancer = new ACOLoadBalancer(virtualMachines, localQueues);
        this.hlbzidLoadBalancer = new HLBZIDLoadBalancer(virtualMachines, globalQueue, localQueues);
    }

    private void initializeVirtualMachines(int vmCount) {
        Random rand = new Random();
        for (int i = 0; i < vmCount; i++) {
            int mips = 1000 + rand.nextInt(1001);
            VirtualMachine vm = new VirtualMachine(i, mips);
            virtualMachines.add(vm);
            localQueues.put(vm, new ConcurrentLinkedQueue<>());
        }
    }
    
    public void addTask(Task task) {
        globalQueue.add(task);
    }
    
    public void addTasks(List<Task> tasks) {
        globalQueue.addAll(tasks);
    }
    
    public synchronized void balance() {
        hlbzidLoadBalancer.balance();
        acoLoadBalancer.balance();
        checkAndRebalance();
    }
    
    private void checkAndRebalance() {
        slaViolationsCount = 0;
        for (VirtualMachine vm : virtualMachines) {
            checkVMSLACompliance(vm);
        }
    }
    
    private void checkVMSLACompliance(VirtualMachine vm) {
        Queue<Task> queue = localQueues.get(vm);
        boolean needsRebalance = false;
        
        for (Task task : queue) {
            double ect = calculateTaskExecutionTime(task, vm);
            // if (ect > task.getDeadline() + SLA_VIOLATION_CHECK_THRESHOLD) {
            if (ect > task.getDeadline() /* + 0 */) {
                slaViolationsCount++;
                needsRebalance = true;
                break;
            }
        }
        
        if (needsRebalance) handleOverloadedVM(vm);
    }
    
    private void handleOverloadedVM(VirtualMachine vm) {
        if (canIncreaseMIPS(vm)) {
            increaseMIPS(vm);
        } else {
            migrateWorkload(vm);
        }
    }
    
    private double calculateTaskExecutionTime(Task task, VirtualMachine vm) {
        return task.getLength() / vm.getMips();
    }
    
    private boolean canIncreaseMIPS(VirtualMachine vm) {
        return vm.getMips() * MIPS_INCREASE_FACTOR <= MAX_MIPS;
    }
    
    private void increaseMIPS(VirtualMachine vm) {
        vm.setMips((int)(vm.getMips() * MIPS_INCREASE_FACTOR));
    }
    
    private void migrateWorkload(VirtualMachine sourceVM) {
        VirtualMachine targetVM = findLeastLoadedVM();
        if (targetVM == null || targetVM == sourceVM) return;
        
        Queue<Task> tasksToMigrate = new LinkedList<>(localQueues.get(sourceVM));
        localQueues.get(sourceVM).clear();
        localQueues.get(targetVM).addAll(tasksToMigrate);
    }
    
    private VirtualMachine findLeastLoadedVM() {
        return virtualMachines.stream()
            .min(Comparator.comparingDouble(this::calculateVMLoad))
            .orElse(null);
    }
    
    private double calculateVMLoad(VirtualMachine vm) {
        return localQueues.get(vm).stream()
            .mapToDouble(task -> task.getLength() / vm.getMips())
            .sum();
    }
    
    public synchronized Map<String, Double> evaluatePerformance() {
        Map<String, Double> metrics = new HashMap<>();
        metrics.put("Makespan", calculateMakespan());
        metrics.put("AverageExecutionTime", calculateAverageExecutionTime());
        metrics.put("ResourceUtilization", calculateResourceUtilization());
        metrics.put("SLAViolations", (double) slaViolationsCount);
        return metrics;
    }
    
    private double calculateMakespan() {
        return virtualMachines.stream()
            .mapToDouble(vm -> localQueues.get(vm).stream()
                .mapToDouble(task -> calculateTaskExecutionTime(task, vm))
                .sum())
            .max().orElse(0);
    }
    
    private double calculateAverageExecutionTime() {
        DoubleSummaryStatistics stats = virtualMachines.stream()
            .flatMap(vm -> localQueues.get(vm).stream())
            .mapToDouble(task -> calculateTaskExecutionTime(task, getTaskVM(task)))
            .summaryStatistics();
        return stats.getAverage();
    }
    
    private VirtualMachine getTaskVM(Task task) {
        return localQueues.entrySet().stream()
            .filter(entry -> entry.getValue().contains(task))
            .map(Map.Entry::getKey)
            .findFirst()
            .orElse(null);
    }
    
    private double calculateResourceUtilization() {
        double totalWorkload = virtualMachines.stream()
            .flatMap(vm -> localQueues.get(vm).stream())
            .mapToDouble(Task::getLength)
            .sum();
        
        double makespan = calculateMakespan();
        if (makespan <= 0) return 0;
        
        double totalCapacity = virtualMachines.stream()
            .mapToDouble(vm -> vm.getMips() * makespan)
            .sum();
        
        double utilization = (totalWorkload / totalCapacity) * 100;
        return Math.min(utilization, 100.0);
    }

    public void placeTasksWithoutBalancing() {
        List<Task> tasks = new ArrayList<>(globalQueue);
        int vmCount = virtualMachines.size();
        int imbalanceVMCount = Math.max(1, (int)(vmCount * 0.4));
        
        IntStream.range(0, tasks.size()).forEach(i -> {
            Task task = tasks.get(i);
            VirtualMachine vm = virtualMachines.get(i < tasks.size() * 0.75 ? 
                i % imbalanceVMCount : 
                imbalanceVMCount + (i % (vmCount - imbalanceVMCount)));
            localQueues.get(vm).add(task);
            globalQueue.remove(task);
        });
    }
}

class HLBZIDLoadBalancer {
    private final List<VirtualMachine> virtualMachines;
    private final Queue<Task> globalQueue;
    private final Map<VirtualMachine, Queue<Task>> localQueues;
    
    public HLBZIDLoadBalancer(List<VirtualMachine> virtualMachines, Queue<Task> globalQueue,
                           Map<VirtualMachine, Queue<Task>> localQueues) {
        this.virtualMachines = virtualMachines;
        this.globalQueue = globalQueue;
        this.localQueues = localQueues;
    }
    
    public void balance() {
        double oct = calculateOptimalCompletionTime();
        allocateTasksByOCT(oct);
        allocateRemainingTasksByEFT();
    }
    
    private double calculateOptimalCompletionTime() {
        double totalLength = globalQueue.stream().mapToDouble(Task::getLength).sum();
        double totalMips = virtualMachines.stream().mapToDouble(VirtualMachine::getMips).sum();
        return totalMips > 0 ? totalLength / totalMips : 0;
    }
    
    // private void allocateTasksByOCT(double oct) {
    //     List<Task> unallocated = new ArrayList<>();
    //     List<Task> allocated = new ArrayList<>();
        
    //     virtualMachines.forEach(vm -> 
    //         new ArrayList<>(globalQueue).forEach(task -> {
    //             if (allocated.contains(task)) return;
                
    //             double ect = task.getLength() / vm.getMips();
    //             if (ect < oct) {
    //                 localQueues.get(vm).add(task);
    //                 allocated.add(task);
    //             } else if (!unallocated.contains(task)) {
    //                 unallocated.add(task);
    //             }
    //         }));
        
    //     globalQueue.removeAll(allocated);
    //     globalQueue.addAll(unallocated);
    // }

    private void allocateTasksByOCT(double oct) {
        List<Task> unallocated = new ArrayList<>();
        Set<Task> allocated = new HashSet<>();

        // for each VM, keep assigning until its load hits oct
        for (VirtualMachine vm : virtualMachines) {
            double vmLoad = localQueues.get(vm).stream()
                           .mapToDouble(task -> task.getLength() / vm.getMips())
                           .sum();

            Iterator<Task> it = globalQueue.iterator();
            while (it.hasNext()) {
                Task task = it.next();
                if (allocated.contains(task)) continue;
                double ect = task.getLength() / vm.getMips();

                // only assign if it won’t push vmLoad over oct
                if (ect < oct && vmLoad + ect <= oct) {
                    localQueues.get(vm).add(task);
                    allocated.add(task);
                    vmLoad += ect;
                    it.remove();
                } else if (!unallocated.contains(task)) {
                    unallocated.add(task);
                }
            }
        }
        // put the leftovers back
        globalQueue.clear();
        globalQueue.addAll(unallocated);
    }

    
    private void allocateRemainingTasksByEFT() {
        new ArrayList<>(globalQueue).forEach(task -> {
            virtualMachines.stream()
                .min(Comparator.comparingDouble(vm -> 
                    calculateEFT(task, vm)))
                .ifPresent(bestVM -> {
                    localQueues.get(bestVM).add(task);
                    globalQueue.remove(task);
                });
        });
    }
    
    private double calculateEFT(Task task, VirtualMachine vm) {
        return localQueues.get(vm).stream()
            .mapToDouble(t -> t.getLength() / vm.getMips())
            .sum() + (task.getLength() / vm.getMips());
    }
}

class ACOLoadBalancer {
    // ACO configuration
    private static final double ALPHA = 1.0;
    private static final double BETA = 2.0;
    private static final double RHO = 0.5;
    private static final int ANT_COUNT = 100;
    private static final int MAX_ITERATIONS = 10;
    
    private final List<VirtualMachine> virtualMachines;
    private final Map<VirtualMachine, Queue<Task>> localQueues;
    private final double[][] pheromones;
    
    public ACOLoadBalancer(List<VirtualMachine> virtualMachines,
                         Map<VirtualMachine, Queue<Task>> localQueues) {
        this.virtualMachines = virtualMachines;
        this.localQueues = localQueues;
        this.pheromones = new double[virtualMachines.size()][virtualMachines.size()];
        initializePheromones();
    }
    
    private void initializePheromones() {
        Arrays.stream(pheromones).forEach(row -> Arrays.fill(row, 1.0));
    }
    
    public void balance() {
        IntStream.range(0, MAX_ITERATIONS).forEach(i -> detectLoadImbalance());
    }

    /**
     * Compute the average VM load for detecting imbalance.
     */
    private double calculateAverageLoad() {
        return virtualMachines.stream()
                              .mapToDouble(this::calculateVMLoad)
                              .average()
                              .orElse(0.0);
   }

    
    private void detectLoadImbalance() {
        double avgLoad = calculateAverageLoad();
        List<VirtualMachine> overloaded = getVMsOverThreshold(avgLoad, 1.2);
        List<VirtualMachine> underloaded = getVMsUnderThreshold(avgLoad, 0.8);
        
        if (!overloaded.isEmpty() && !underloaded.isEmpty()) {
            balanceWithACO(overloaded, underloaded);
        }
    }
    
    private List<VirtualMachine> getVMsOverThreshold(double avg, double threshold) {
        return virtualMachines.stream()
            .filter(vm -> calculateVMLoad(vm) > avg * threshold)
            .collect(Collectors.toList());
    }
    
    private List<VirtualMachine> getVMsUnderThreshold(double avg, double threshold) {
        return virtualMachines.stream()
            .filter(vm -> calculateVMLoad(vm) < avg * threshold)
            .collect(Collectors.toList());
    }
    
    // ... rest of ACO implementation remains similar with stream API improvements ...
     private void balanceWithACO(List<VirtualMachine> overloadedVMs, List<VirtualMachine> underloadedVMs) {
        for (VirtualMachine sourceVM : overloadedVMs) {
            // Generate forward ants to find candidate VMs
            List<VirtualMachine> candidateVMs = generateForwardAnts(sourceVM, underloadedVMs);
            
            if (!candidateVMs.isEmpty()) {
                // Generate backward ants to update pheromones
                generateBackwardAnts(sourceVM, candidateVMs);
                
                // Migrate tasks based on pheromone values
                migrateTasksBasedOnPheromones(sourceVM, candidateVMs);
            }
        }
        
        // Evaporate pheromones
        evaporatePheromones();
    }
    
    private List<VirtualMachine> generateForwardAnts(VirtualMachine sourceVM, List<VirtualMachine> underloadedVMs) {
        List<VirtualMachine> candidateVMs = new ArrayList<>();
        
        for (int ant = 0; ant < ANT_COUNT; ant++) {
            double[] probabilities = calculateMovingProbabilities(sourceVM, underloadedVMs);
            
            // Select VM based on probabilities
            VirtualMachine selectedVM = selectVM(underloadedVMs, probabilities);
            
            if (selectedVM != null && !candidateVMs.contains(selectedVM)) {
                candidateVMs.add(selectedVM);
            }
        }
        
        return candidateVMs;
    }
    
    private double[] calculateMovingProbabilities(VirtualMachine sourceVM, List<VirtualMachine> targetVMs) {
        double[] probabilities = new double[targetVMs.size()];
        double sum = 0;
        
        for (int i = 0; i < targetVMs.size(); i++) {
            VirtualMachine targetVM = targetVMs.get(i);
            double pheromone = pheromones[sourceVM.getId()][targetVM.getId()];
            double heuristic = 1.0 / (calculateVMLoad(targetVM) + 0.1); // Avoid division by zero
            
            probabilities[i] = Math.pow(pheromone, ALPHA) * Math.pow(heuristic, BETA);
            sum += probabilities[i];
        }
        
        // Normalize probabilities
        if (sum > 0) {
            for (int i = 0; i < probabilities.length; i++) {
                probabilities[i] /= sum;
            }
        }
        
        return probabilities;
    }
    
    private VirtualMachine selectVM(List<VirtualMachine> vms, double[] probabilities) {
        double r = Math.random();
        double cumulativeProbability = 0;
        
        for (int i = 0; i < vms.size(); i++) {
            cumulativeProbability += probabilities[i];
            if (r <= cumulativeProbability) {
                return vms.get(i);
            }
        }
        
        return vms.get(vms.size() - 1); // Default to last VM if something goes wrong
    }
    
    private void generateBackwardAnts(VirtualMachine sourceVM, List<VirtualMachine> candidateVMs) {
        for (VirtualMachine targetVM : candidateVMs) {
            // Update pheromones based on load difference
            double loadDifference = calculateVMLoad(sourceVM) - calculateVMLoad(targetVM);
            
            if (loadDifference > 0) {
                // Increase pheromone for paths that lead to better balance
                pheromones[sourceVM.getId()][targetVM.getId()] += loadDifference;
            }
        }
    }
    
    private void migrateTasksBasedOnPheromones(VirtualMachine sourceVM, List<VirtualMachine> targetVMs) {
        if (localQueues.get(sourceVM).isEmpty()) {
            return;
        }
        
        // Find best target VM based on pheromones
        VirtualMachine bestTargetVM = null;
        double maxPheromone = -1;
        
        for (VirtualMachine targetVM : targetVMs) {
            double pheromoneValue = pheromones[sourceVM.getId()][targetVM.getId()];
            if (pheromoneValue > maxPheromone) {
                maxPheromone = pheromoneValue;
                bestTargetVM = targetVM;
            }
        }
        
        if (bestTargetVM != null) {
            // Migrate task from source to target
            Task taskToMigrate = localQueues.get(sourceVM).poll();
            if (taskToMigrate != null) {
                localQueues.get(bestTargetVM).add(taskToMigrate);
                
                // Increase pheromone if migration was successful
                pheromones[sourceVM.getId()][bestTargetVM.getId()] *= 1.1;
            }
        }
    }
    
    private void evaporatePheromones() {
        for (int i = 0; i < virtualMachines.size(); i++) {
            for (int j = 0; j < virtualMachines.size(); j++) {
                pheromones[i][j] *= (1 - RHO);
                
                // Ensure minimum pheromone level
                if (pheromones[i][j] < 0.1) {
                    pheromones[i][j] = 0.1;
                }
            }
        }
    }
    
    private double calculateVMLoad(VirtualMachine vm) {
        double totalLength = 0;
        for (Task task : localQueues.get(vm)) {
            totalLength += task.getLength();
        }
        return vm.getMips() > 0 ? totalLength / vm.getMips() : 0;
    }
}

class Task {
    private final int id;
    private final double length;
    private final double deadline;
    
    public Task(int id, double length, double deadline) {
        this.id = id;
        this.length = length;
        this.deadline = deadline;
    }
    
    // Getters remain same
    public int getId() {
        return id;
    }
    
    public double getLength() {
        return length;
    }
    
    public double getDeadline() {
        return deadline;
    }
}

class VirtualMachine {
    private final int id;
    private int mips;
    
    public VirtualMachine(int id, int mips) {
        this.id = id;
        this.mips = mips;
    }
    
    // Getters/setters remain same
     public int getId() {
        return id;
    }
    
    public int getMips() {
        return mips;
    }
    
    public void setMips(int mips) {
        this.mips = mips;
    }
}