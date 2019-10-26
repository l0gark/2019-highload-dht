package ru.mail.polis;

import org.junit.jupiter.api.Test;
import ru.mail.polis.service.Node;
import ru.mail.polis.service.Topology;

import java.awt.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class RendezvousTest extends TestBase {
    private static final Point[] NODES = {
            new Point(1500, 10),
            new Point(5000, 15),
            new Point(10000, 67),
            new Point(10000, 50),
            new Point(50000, 150)
    };

    /**
     * Тест на равномерность распределения.
     * В map считается сколько раз на какую ноду пошёл ключ.
     * Дальше считается среднее значение, математическое ожидание
     * и среднее квадратичное отклонение, для оценки амплитуды значений.
     * Максимальная погрешность выбрана эмпирическим путём.
     */
    @Test
    void uniformityTest() {

        for (int indexTest = 0; indexTest < NODES.length; indexTest++) {
            final int countKeys = NODES[indexTest].x;
            final int countNode = NODES[indexTest].y;
            final String name = "http://localhost:";

            final Set<String> nodes = new HashSet<>(countNode << 1);
            final Map<String, Integer> map = new HashMap<>();
            for (int i = 0; i < countNode; i++) {
                final String node = name + (8080 + i);
                nodes.add(node);
                map.put(node, 0);
            }

            final Topology<String> topology = new Node(nodes, name);

            for (int i = 0; i < countKeys; i++) {
                final ByteBuffer key = randomKeyBuffer();
                final String node = topology.primaryFor(key);
                assertTrue(map.containsKey(node));
                map.put(node, map.get(node) + 1);
            }

            double matWait = 0;
            int sum = 0;

            for (final int value : map.values()) {
                matWait += (double) value * value / countKeys;
                sum += value;
            }

            final double avg = (double) sum / map.size();

            double avgSqrDeviation = 0;

            for (int value : map.values()) {
                avgSqrDeviation += (value - avg) * (value - avg);
            }

            avgSqrDeviation = Math.sqrt(avgSqrDeviation / map.size());

            final double[] errors = {
                    avgSqrDeviation / matWait,
                    avgSqrDeviation / avg,
                    Math.abs(matWait - avg) / Math.min(matWait, avg)
            };

            final double MAX_ERROR = 0.15;

            for (final double error : errors) {
                assertTrue(error < MAX_ERROR);
            }
        }
    }
}
