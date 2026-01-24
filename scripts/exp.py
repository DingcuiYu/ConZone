import subprocess
import os

# Configurations
CONFIG_NAME = "block"
LOG_DIR = f"log/fio_results/{CONFIG_NAME}"
BS_LIST = [
    "4k",
    "8k",
    "16k",
    "32k",
    "64k",
    "128k",
    "256k",
    "512k",
    "1M",
    "2M",
    "4M",
    "8M",
    "16M",
]
IO_RANGE = ["64m"]  # "4m", "64m"
IODEPTHS = [1, 8, 32]
MOUNT_CMD = "sudo python3 scripts/mount.py"
UMOUNT_CMD = "sudo python3 scripts/umount.py"
FIO_STATUS_INTERVAL = ""


def run_shell_command(command, description, log_path=None):
    """
    Executes a Shell command.
    If log_path is provided (for FIO commands), the output is displayed to the terminal
    in real-time and written to the file simultaneously.
    If log_path is not provided (for mount/umount/dmesg), the output is captured.
    """
    print(f"Running: {description} ({command})")

    if log_path:
        print(
            f"Note: The output of this command will be displayed in real-time on the terminal and logged to the file: {log_path}"
        )

        # Use Popen to control the real-time stream
        try:
            # shell=True allows for command string with pipe, but we don't need tee anymore.
            # We run the command directly.
            # We use text=True (universal_newlines=True) for text mode.
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Merge stderr into stdout stream
                shell=True,
                text=True,
                bufsize=1,  # Line buffering
            )

            full_output = []

            # Read and print output in real-time, while collecting it into the full_output list
            for line in process.stdout:
                print(line, end="", flush=True)
                full_output.append(line)

            process.wait()

            if process.returncode != 0:
                error_msg = f"Command failed with return code: {process.returncode}"
                # In this mode, the actual error is already in full_output

            else:
                error_msg = ""  # Success

            # Write the complete output content to the log file
            with open(log_path, "w") as f:
                f.writelines(full_output)

            # Return empty strings for stdout/stderr because the output was already handled
            return "", error_msg

        except Exception as e:
            error_msg = (
                f"An unexpected error occurred while executing {command}: {str(e)}"
            )
            print(f"Error: {error_msg}")
            return "", error_msg

    else:
        try:
            result = subprocess.run(
                command, capture_output=True, text=True, shell=True, check=True
            )
            return result.stdout, result.stderr
        except subprocess.CalledProcessError as e:
            error_msg = f"Command failed: {command}\nError message:\n{e.stderr}"
            print(f"Error: {error_msg}")
            return "", error_msg
        except Exception as e:
            error_msg = (
                f"An unexpected error occurred while executing {command}: {str(e)}"
            )
            print(f"Error: {error_msg}")
            return "", error_msg


def execute_fio_test_unit(task_name, fio_command, prefill_cmd="", filesystem=True):
    """
    Executes a single FIO test unit, including mount, FIO execution, unmount, and logging.
    is_fio_command: Flag indicating if it is an FIO command; FIO command output is written to the FIO log file, others are not.
    """
    print("-" * 60)
    print(f"Starting FIO Test Unit: {task_name}")

    fio_log_path = os.path.join(LOG_DIR, f"{task_name}_fio_result.txt")
    dmesg_log_path = os.path.join(LOG_DIR, f"{task_name}_dmesg_output.txt")

    if filesystem:
        run_shell_command(MOUNT_CMD, "Mount filesystem")
    else:
        run_shell_command(MOUNT_CMD + " rawdevice", "Raw device test")

    # 2. Clear dmesg buffer
    run_shell_command("sudo dmesg -c", "Clear dmesg")

    # 3.0 Execute prefill if needed
    if prefill_cmd != "":
        prefill_fio_log_path = os.path.join(
            LOG_DIR, f"{task_name}_prefill_fio_result.txt"
        )
        prefill_dmesg_log_path = os.path.join(
            LOG_DIR, f"{task_name}_prefill_dmesg_output.txt"
        )

        print("\n--- Task Prefill ---")
        prefill_stdout, prefill_stderr = run_shell_command(
            prefill_cmd, f"FIO Command: {prefill_cmd}", log_path=prefill_fio_log_path
        )

        prefill_dmesg_stdout, _ = run_shell_command(
            "sudo dmesg -c", "Capture dmesg output"
        )

        if prefill_stderr:
            print(
                f"FIO command finished, potential errors (Please check {prefill_fio_log_path}）。"
            )
        else:
            print(
                f"FIO command executed successfully, result written to: {prefill_fio_log_path}"
            )

        with open(prefill_dmesg_log_path, "w") as f:
            f.write(prefill_dmesg_stdout)
        print(f"Prefill Dmesg result written to: {prefill_dmesg_log_path}")

    # 3. Run FIO command
    # fio_stdout/fio_stderr is now an error string or empty, not the command output
    fio_stdout, fio_stderr = run_shell_command(
        fio_command, f"FIO 命令: {fio_command}", log_path=fio_log_path
    )

    if filesystem:
        run_shell_command(UMOUNT_CMD, "Unmount filesystem")
    else:
        run_shell_command(UMOUNT_CMD + " rawdevice", "Unmount the emulator")

    # 5. Capture dmesg (after Umount)
    dmesg_stdout, _ = run_shell_command("sudo dmesg -c", "Capture dmesg output")

    # 6. Report FIO status
    if fio_stderr:
        print(f"FIO command finished, potential errors (Please check {fio_log_path}).")
    else:
        print(f"FIO command executed successfully, result written to: {fio_log_path}")

    # 7. Write dmesg log
    with open(dmesg_log_path, "w") as f:
        f.write(dmesg_stdout)
    print(f"Dmesg result written to: {dmesg_log_path}")


def task_a():
    """Task A: sequential write"""
    print("\n" * 3 + "=" * 80)
    print("Starting Task A: Sequential Write")
    print("=" * 80)
    for bs in BS_LIST:
        cpu_allowed = "--cpus_allowed=35"
        size = 1024  # MiB
        fio_cmd = (
            f"sudo fio --name=A_{bs} --filename=mnt/test --buffered=0 "
            f"--numjobs=1 --thread=1 --iodepth=1 --iodepth_batch_submit=1 "
            f"--rw=write --bs={bs} --size={size}m --io_size=1g "
            f"--ioengine=psync {FIO_STATUS_INTERVAL} --group_reporting {cpu_allowed} "
            f"--output-format=json"
        )
        execute_fio_test_unit(f"A_{bs}", fio_cmd, filesystem=True)


def task_b():
    """Task B: random read"""
    print("\n" * 3 + "=" * 80)
    print("Starting Task B: Random Read")
    print("=" * 80)
    for iosize in IO_RANGE:
        cpu_allowed = "--cpus_allowed=35"
        if CONFIG_NAME == "conzone":
            zonemode = "--zonemode=zbd"
            size = "8z"
        else:
            zonemode = ""
            size = "4224m"
        prefill_cmd = (
            f"sudo fio --name=B_prefill_{iosize} --filename=/dev/nvme2n2 --buffered=0 "
            f"--numjobs=1 --thread=1 --iodepth=1 --iodepth_batch_submit=1 "
            f"--rw=write --bs=192k --size={size} {zonemode} "
            f"--ioengine=psync {FIO_STATUS_INTERVAL} --group_reporting {cpu_allowed} "
        )
        fio_cmd = (
            f"sudo fio --name=B_{iosize} --filename=/dev/nvme2n2 --buffered=0 "
            f"--numjobs=1 --thread=1 --iodepth=1 --iodepth_batch_submit=1 "
            f"--rw=randread --bs=4k --size={iosize} --io_size=4g {zonemode} "
            f"--ioengine=psync {FIO_STATUS_INTERVAL} --group_reporting {cpu_allowed} "
        )
        execute_fio_test_unit(
            f"B_{iosize}", fio_cmd, filesystem=False, prefill_cmd=prefill_cmd
        )


def main():
    """Main function: set up environment and run all tasks."""
    # Ensure the log directory exists
    os.makedirs(LOG_DIR, exist_ok=True)
    print(f"Log directory '{LOG_DIR}' is ready.")
    print(
        "Note: The script executes all commands with 'sudo', please ensure you have the necessary permissions."
    )
    if os.geteuid() != 0:
        print(
            "❌ WARNING: Your I/O commands typically require root privileges. Please run this script using 'sudo python3 xxx.py'."
        )
        return

    # Execute all tasks
    task_b()

    print("\n" * 3 + "=" * 80)
    print("All tasks completed.")
    print(f"All log files are saved in the '{LOG_DIR}/' directory.")
    print("=" * 80)


if __name__ == "__main__":
    main()
