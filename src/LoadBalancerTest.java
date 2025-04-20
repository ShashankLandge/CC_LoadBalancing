import java.util.*;
import java.util.stream.*;

public class LoadBalancerTest {
    private static final Formatter fmt = new Formatter();
    
    public static void main(String[] args) {
        testSimpleScenario();
        testLoadImbalanceScenario();
        testSLAViolationScenario();
        testScalabilityScenario();
    }

    private static void testSimpleScenario() {
        printTestHeader("Simple Scenario", 3, 10);
        HybridLoadBalancer lb = new HybridLoadBalancer(3);
        
        List<Task> tasks = IntStream.range(0, 10)
            .mapToObj(i -> new Task(i, 1000 + Math.random() * 2000, 3 + Math.random() * 2))
            .collect(Collectors.toList());
        
        lb.addTasks(tasks);
        lb.placeTasksWithoutBalancing();
        
        printMetrics("Initial State", lb.evaluatePerformance());
        lb.balance();
        printMetrics("After Balancing", lb.evaluatePerformance());
    }

    private static void testLoadImbalanceScenario() {
        printTestHeader("Load Imbalance Scenario", 5, 20);
        HybridLoadBalancer lb = new HybridLoadBalancer(5);
        
        List<Task> tasks = IntStream.range(0, 20)
            .mapToObj(i -> {
                double length = i < 15 ? 2000 + Math.random()*1000 : 500 + Math.random()*500;
                return new Task(i, length, 3 + Math.random()*2);
            }).collect(Collectors.toList());
        
        lb.addTasks(tasks);
        lb.placeTasksWithoutBalancing();
        
        printMetrics("Before Balancing", lb.evaluatePerformance());
        lb.balance();
        printMetrics("After Balancing", lb.evaluatePerformance());
    }

    private static void testSLAViolationScenario() {
        printTestHeader("SLA Violation Scenario", 3, 15);
        HybridLoadBalancer lb = new HybridLoadBalancer(3);
        
        List<Task> tasks = IntStream.range(0, 15)
            .mapToObj(i -> new Task(i, 2000 + Math.random()*1000, 1 + Math.random()))
            .collect(Collectors.toList());
        
        lb.addTasks(tasks);
        lb.placeTasksWithoutBalancing();
        
        printMetrics("Initial State", lb.evaluatePerformance());
        lb.balance();
        printMetrics("After Mitigation", lb.evaluatePerformance());
    }

    private static void testScalabilityScenario() {
        printTestHeader("Scalability Scenario", 10, 100);
        HybridLoadBalancer lb = new HybridLoadBalancer(10);
        
        List<Task> tasks = IntStream.range(0, 100)
            .mapToObj(i -> new Task(i, 1000 + Math.random()*3000, 2 + Math.random()*3))
            .collect(Collectors.toList());
        
        long start = System.currentTimeMillis();
        lb.addTasks(tasks);
        lb.balance();
        long duration = System.currentTimeMillis() - start;
        
        printMetrics("Final Metrics", lb.evaluatePerformance());
        System.out.println(fmt.format("Balancing Time: %,d ms%n", duration));
    }

    private static void printTestHeader(String scenario, int vms, int tasks) {
        System.out.println("\n" + "=".repeat(50));
        System.out.printf("=== %s (%d VMs, %d tasks) ===%n", scenario, vms, tasks);
        System.out.println("=".repeat(50));
    }

    private static void printMetrics(String label, Map<String, Double> metrics) {
        System.out.printf("%n%s:%n", label);
        System.out.printf("  Makespan: %8.2f sec%n", metrics.get("Makespan"));
        System.out.printf("  Avg Exec Time: %6.2f sec%n", metrics.get("AverageExecutionTime"));
        System.out.printf("  Utilization: %8.1f%%%n", metrics.get("ResourceUtilization"));
        System.out.printf("  SLA Violations: %4d%n", metrics.get("SLAViolations").intValue());
    }
}