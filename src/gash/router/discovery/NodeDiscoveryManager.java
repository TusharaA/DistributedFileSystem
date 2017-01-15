package gash.router.discovery;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;

public class NodeDiscoveryManager {

	public static String findSelfIp() {
		String ip = null;
		try {
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			while (interfaces.hasMoreElements()) {
				NetworkInterface iface = interfaces.nextElement();
				// filters out 127.0.0.1 and inactive interfaces
				if (iface.isLoopback() || !iface.isUp())
					continue;

				Enumeration<InetAddress> addresses = iface.getInetAddresses();
				while (addresses.hasMoreElements()) {
					InetAddress addr = addresses.nextElement();
					ip = addr.getHostAddress();
					// System.out.println(iface.getDisplayName() + "Local IP: "
					// + ip);
					// return(ip);
				}
				return ip;
			}
		} catch (SocketException e) {
			throw new RuntimeException(e);
		}
		return (null);
	}

	public static ArrayList<InetAddress> checkHosts() throws UnknownHostException, IOException {

		ArrayList<InetAddress> liveIps = new ArrayList<InetAddress>();
		InetAddress localhost = InetAddress.getByName(findSelfIp()); // InetAddress.getLocalHost();

		// this code assumes IPv4 is used
		byte[] ip = localhost.getAddress();
		for (int i = 1; i <= 254; i++) {
			ip[3] = (byte) i;
			// System.out.println(ip);

			InetAddress address = InetAddress.getByAddress(ip);
			if (address.isReachable(10)) {
				// machine is turned on and can be pinged
				// System.out.println("Pingable : "+address.getHostAddress());
				liveIps.add(address);
			} else if (!address.getHostAddress().equals(address.getHostName())) {
				// machine is known in a DNS lookup
				// System.out.println(" Visible : "+address.getHostAddress());
			} else {
				// the host address and host name are equal, meaning the host
				// name could not be resolved
				// System.out.println(" Not Resolved :
				// "+address.getHostAddress());
			}
		}
		return (liveIps);
	}

	public static void main(String args[]) {

		try {
			checkHosts();
			// System.out.println(findSelfIp());
			// findSelfIp();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}