import hid
import time


def open_and_read(vendor_id, product_id):
    for i in range(10):
        try:
            dev = hid.device()
            dev.open(vendor_id, product_id)
            dev.set_nonblocking(0)
            report = dev.get_input_report(0, 512)
            print(report)
            dev.close()
            return report
        except Exception as e:
            print(f"Error opening device: {e}")
    time.sleep(1)


def print_summary(device):
    values = [f"{k}={v}" for k, v in device.items()]
    print(" | ".join(values))


def main():
    for device in hid.enumerate():
        print_summary(device)

        if "HOTSPOT" in device.get("manufacturer_string") or "HANVON" in device.get(
            "manufacturer_string"
        ):
            report = open_and_read(device.get("vendor_id"), device.get("product_id"))
            if report:
                raw = report[1:]
                version = bytes(raw).split(b"\x00", 1)[0].decode("ascii")
                print(f"Version: {version}")
        else:
            print(f"Device {device.get('manufacturer_string')} is not a HOTSPOT")

        print("-" * 70)
