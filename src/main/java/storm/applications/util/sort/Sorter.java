package storm.applications.util.sort;

import java.util.Comparator;
import java.util.List;

/**
 * Sort the arbitrary object list with given comparator.
 * @author yexijiang
 * @param <T>
 *
 */
public class Sorter<T> {
    public static void quicksort(List list, Comparator comp) {
        quicksort(list, 0, list.size() - 1, comp);
    }

    private static int partition(List list, int left, int right, Comparator comp) {
        int bar = left - 1;
        Object pivot = list.get(right);

        for (int i = left; i < right; ++i) {
            if (comp.compare(list.get(i), pivot) < 0) {
                ++bar;
                swap(list, i, bar);
            }
        }

        swap(list, bar + 1, right);
        return bar + 1;
    }

    private static void quicksort(List list, int left, int right, Comparator comp) {
        if (left < right) {
            int pivot = partition(list, left, right, comp);
            quicksort(list, left, pivot - 1, comp);
            quicksort(list, pivot + 1, right, comp);
        }
    }

    private static void swap(List list, int first, int second) {
        Object tmp = list.get(first);
        list.set(first, list.get(second));
        list.set(second, tmp);
    }
}