/**
 * Created by dpavlov on 13.10.2017.
 */
public class Developer {
    String name;
    String grade;

    public Developer(String name) {
        this(name, "Senior");
    }

    public Developer(String name, String grade) {
        this.name = name;
        this.grade = grade;
    }

    @Override public String toString() {
        return "Developer{" +
            "name='" + name + '\'' +
            ", grade='" + grade + '\'' +
            '}';
    }
}
