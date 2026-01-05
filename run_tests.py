#!/usr/bin/env python
"""
테스트 실행 스크립트

사용법:
    python run_tests.py              # 전체 테스트
    python run_tests.py market_data  # 특정 모듈 테스트
    python run_tests.py --cov        # 커버리지 포함
"""
import sys
import subprocess


def main():
    args = sys.argv[1:]
    
    cmd = ["python", "-m", "pytest"]
    
    if not args:
        # 전체 테스트
        cmd.append("tests/")
    elif args[0] == "--cov":
        # 커버리지 포함
        cmd.extend(["--cov=src", "--cov-report=term-missing", "tests/"])
    else:
        # 특정 모듈 테스트
        module = args[0]
        cmd.append(f"tests/test_{module}.py")
    
    cmd.extend(["-v", "--tb=short"])
    
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd)


if __name__ == "__main__":
    main()
