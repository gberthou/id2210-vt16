import java.util.ArrayList;
import java.util.List;

/**
 * Created by Michael on 24/05/2016.
 */
public class yolo {
    public static void main(String[] args) {
        List<Integer> d = new ArrayList<>();
        d.add(1); d.add(3);
        List<Integer> f = new ArrayList<>();
        for(int i : d)
            f.add(i);
        d.clear();
        System.out.println(f);
    }
}
