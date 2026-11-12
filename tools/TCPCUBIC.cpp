#include <iostream>
#include <cmath>
#include <algorithm>
using namespace std;
class TCPCUBIC {
private:
    double cwnd;
    double wmax;
    double C;
    double beta;
    int t;
    double K;
    bool tcp_friendly;
public:
    TCPCUBIC(double init_cwnd = 10.0, double cubic_c = 0.4,
             double b = 0.7, bool tf = true) {
        cwnd = init_cwnd;
        wmax = 0.0;
        C = cubic_c;
        beta = b;
        t = 0;
        K = 0.0;
        tcp_friendly = tf;
    }
    void onAck() {
        t++;

        if (wmax == 0.0) {
            // No previous loss, use simple growth
            cwnd += 1.0;
        } else {
            // CUBIC function: W(t) = C(t-K)^3 + Wmax
            double time_diff = t - K;
            double target = C * pow(time_diff, 3) + wmax;

            // TCP-friendly mode check
            double tcp_cwnd = cwnd + (3 * beta) / (2 - beta) * (t / cwnd);

            if (tcp_friendly && target < tcp_cwnd) {
                cwnd = tcp_cwnd;
            } else {
                cwnd = target;
            }

            // Ensure cwnd doesn't decrease
            if (cwnd < wmax * beta) {
                cwnd = wmax * beta;
            }
        }
    }
    void onLoss() {
        wmax = cwnd;
        cwnd = cwnd * beta;
        t = 0;

        // Calculate K: time to reach Wmax again
        K = cbrt((wmax - cwnd) / C);
    }
    double getCwnd() { return cwnd; }
    double getWmax() { return wmax; }

    void printState(int rtt) {
        cout << rtt << "\t" << (int)cwnd << "\t\t" << (int)wmax
             << "\t\t" << t << endl;
    }
};
int main() {
    TCPCUBIC cubic(10.0);
    int maxRTT = 15;
    int lossRound1 = 5;
    int lossRound2 = 12;

    cout << "RTT\tcwnd\t\tWmax\t\tTime(t)" << endl;

    for (int r = 1; r <= maxRTT; r++) {
        cubic.printState(r);

        if (r == lossRound1) {
            cout << "=> Packet loss detected at RTT " << r << endl;
            cubic.onLoss();
            cout << "=> cwnd reduced, Wmax updated, epoch reset" << endl;
        } else if (r == lossRound2) {
            cout << "=> Packet loss detected at RTT " << r << endl;
            cubic.onLoss();
            cout << "=> cwnd reduced, new epoch begins" << endl;
        } else {
            cubic.onAck();
        }
    }

    return 0;
}