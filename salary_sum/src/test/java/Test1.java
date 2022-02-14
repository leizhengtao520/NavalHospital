public class Test1 {
    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i <= 100; i++) {
            printSchedule(i);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 进度条总长度
     */
    private static int TOTLE_LENGTH = 50;
    public static void printSchedule(int percent){
        for (int i = 0; i < TOTLE_LENGTH + 10; i++) {
            System.out.print("\b");
        }
        //░▒
        int now = TOTLE_LENGTH * percent / 100;
        for (int i = 0; i < now; i++) {
            System.out.print(">");
        }
        for (int i = 0; i < TOTLE_LENGTH - now; i++) {
            System.out.print(" ");
        }
        System.out.print("  " + percent + "%");
    }

}
