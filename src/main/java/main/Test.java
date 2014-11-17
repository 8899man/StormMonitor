package main;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// ArrayList<Integer> arr = new Test().sort();
		// System.out.println(arr);
		String s = "mmllp";
		s = s.substring(0, 2);
		boolean mm = s.equalsIgnoreCase("MM");
		System.out.println(s+" "+ mm);
	}

	ArrayList<Integer> sort() {
		ArrayList<Integer> arr = new ArrayList<Integer>();
		for (int i = 10; i > 0; i--) {
			arr.add(i);
		}
		Collections.sort(arr);
		return arr;
	}
}
