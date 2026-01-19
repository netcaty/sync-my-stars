#!/usr/bin/env python3
"""
同步 GitHub starred 仓库到 Codeberg
修复 NoneType 错误和权限问题
"""

import os
import sys
import json
import requests
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple

def simple_sanitize(text: str) -> str:
    """简单的URL脱敏函数"""
    import re
    
    def replace_token(match):
        url = match.group(0)
        
        # 检查是否有冒号（username:token格式）
        if '://' in url:
            # 提取协议后面的部分
            parts = url.split('://', 1)
            protocol = parts[0]
            rest = parts[1]
            
            # 找到@符号的位置
            at_pos = rest.find('@')
            if at_pos != -1:
                # 获取@前面的凭证部分
                credentials = rest[:at_pos]
                
                # 检查是否有冒号分隔用户名和token
                if ':' in credentials:
                    # username:token格式
                    username = credentials.split(':')[0]
                    rest = f"{username}:***{rest[at_pos:]}"
                else:
                    # 只有token格式
                    rest = f"***{rest[at_pos:]}"
                
                return f"{protocol}://{rest}"
        
        return url
    
    # 匹配包含@的URL
    pattern = r'https?://[^\s]+@[^\s]+'
    return re.sub(pattern, replace_token, text)


def log(message: str):
    """简单的日志函数"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {simple_sanitize(message)}")

def run_command(cmd: List[str], cwd: Optional[Path] = None, ignore_errors: bool = False) -> Tuple[bool, str]:
    """执行 shell 命令并返回结果"""
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=300
        )
        log(f"cmd: {cmd}, stderr: {result.stderr}, stdout: {result.stdout}")
        if result.returncode != 0 and not ignore_errors:
            return False, f"{result.stderr}"
        return True, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, f"命令超时: {' '.join(cmd)}"
    except Exception as e:
        return False, f"执行命令时出错: {str(e)}"

class SyncManager:
    def __init__(self):
        # 从环境变量读取配置
        self.github_token = os.getenv('GITHUB_TOKEN')
        self.codeberg_username = os.getenv('CODEBERG_USERNAME')
        self.codeberg_token = os.getenv('CODEBERG_TOKEN')
        self.full_sync = os.getenv('FULL_SYNC', 'false').lower() == 'true'
        
        # 验证配置
        if not self.github_token:
            log("错误: 缺少 GITHUB_TOKEN 环境变量")
            log("请确保使用个人访问令牌 (PAT) 而不是默认的 GITHUB_TOKEN")
            sys.exit(1)
        if not self.codeberg_username:
            log("错误: 缺少 CODEBERG_USERNAME 环境变量")
            sys.exit(1)
        if not self.codeberg_token:
            log("错误: 缺少 CODEBERG_TOKEN 环境变量")
            sys.exit(1)
        
        # API 配置 - 使用个人访问令牌
        self.github_headers = {
            'Authorization': f'token {self.github_token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        self.codeberg_headers = {
            'Authorization': f'token {self.codeberg_token}',
            'Content-Type': 'application/json'
        }
        
        # 工作目录
        self.workspace = Path('.')
        
        # 状态文件路径
        self.state_file = self.workspace / '.github' / 'sync_state.json'
        self.state_file.parent.mkdir(exist_ok=True)
        
        # 临时存储仓库的目录
        self.repos_dir = Path('/tmp/github-backup/repos')
        self.repos_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载状态
        self.state = self.load_state()
        
        # 统计信息
        self.stats = {
            'total': 0,
            'new': 0,
            'updated': 0,
            'skipped': 0,
            'failed': 0,
            'start_time': datetime.now()
        }
        
        # 测试 GitHub API 连接
        self.test_github_connection()
    
    def test_github_connection(self):
        """测试 GitHub API 连接和权限"""
        log("测试 GitHub API 连接...")
        try:
            # 测试获取用户信息
            response = requests.get(
                'https://api.github.com/user',
                headers=self.github_headers,
                timeout=10
            )
            
            if response.status_code == 200:
                user_info = response.json()
                log(f"✓ 连接成功，用户: {user_info.get('login')}")
            else:
                log(f"✗ 连接失败: {response.status_code}")
                log(f"错误信息: {response.text}")
                sys.exit(1)
                
        except Exception as e:
            log(f"✗ 连接测试失败: {e}")
            sys.exit(1)
    
    def load_state(self) -> Dict:
        """从项目根目录加载状态文件"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state_data = json.load(f)
                    # 兼容新旧格式
                    if 'repositories' in state_data:
                        repos = state_data['repositories']
                    else:
                        repos = state_data
                    
                    log(f"从 {self.state_file} 加载状态，包含 {len(repos)} 个仓库记录")
                    return repos
            except json.JSONDecodeError as e:
                log(f"警告: 状态文件格式错误，创建新的: {e}")
                return {}
            except Exception as e:
                log(f"警告: 加载状态文件失败: {e}")
                return {}
        else:
            log("状态文件不存在，创建新的")
            return {}
    
    def save_state(self):
        """保存状态文件到项目根目录"""
        try:
            # 准备要保存的数据
            data_to_save = {
                'metadata': {
                    'last_updated': datetime.now().isoformat(),
                    'total_repos': len(self.state),
                    'version': '1.0'
                },
                'repositories': self.state
            }
            
            # 保存到文件
            with open(self.state_file, 'w') as f:
                json.dump(data_to_save, f, indent=2, ensure_ascii=False)
            
            log(f"状态已保存到: {self.state_file}")
            
        except Exception as e:
            log(f"错误: 保存状态文件失败: {e}")
            # 在失败时尝试保存简化版本
            try:
                with open(self.state_file, 'w') as f:
                    json.dump(self.state, f, indent=2)
                log("已保存简化版本的状态文件")
            except Exception as e2:
                log(f"保存简化版本也失败: {e2}")
    
    def get_starred_repos(self) -> List[Dict]:
        """获取所有 starred 仓库（处理分页）"""
        repos = []
        page = 1
        per_page = 30  # 降低每页数量避免请求过大
        
        log("获取 GitHub starred 仓库列表...")
        
        while True:
            try:
                url = f"https://api.github.com/user/starred"
                params = {
                    'page': page,
                    'per_page': per_page,
                    'sort': 'updated'
                }
                
                response = requests.get(
                    url, 
                    headers=self.github_headers,
                    params=params,
                    timeout=30
                )
                
                if response.status_code != 200:
                    log(f"获取 starred 仓库失败: {response.status_code}")
                    log(f"响应: {response.text}")
                    log("请确保使用的令牌具有 'user' 范围的 'read:user' 权限")
                    break
                
                page_repos = response.json()
                if not page_repos:
                    break
                
                for repo_data in page_repos:
                    # 修复：检查 repo_data 是否为 None
                    if repo_data is None:
                        log("警告: 跳过 None 仓库数据")
                        continue
                    
                    try:
                        # 修复：使用安全的字典访问方式
                        repo_info = {
                            'full_name': repo_data.get('full_name'),
                            'name': repo_data.get('name'),
                            'updated_at': repo_data.get('updated_at'),
                            'clone_url': repo_data.get('clone_url'),
                            'description': (repo_data.get('description', '')[:100] if repo_data.get('description') else '') + f"(Mirror of {repo_data.get('html_url')})"
                        }
                        
                        # 检查必需字段是否存在
                        if not repo_info['full_name'] or not repo_info['name']:
                            log(f"警告: 跳过无效仓库数据: {repo_data}")
                            continue
                            
                        repos.append(repo_info)
                        
                    except Exception as e:
                        log(f"警告: 处理仓库数据时出错，跳过: {e}")
                        log(f"问题数据: {repo_data}")
                        continue
                
                log(f"已获取第 {page} 页，共 {len(repos)} 个仓库")
                
                # 检查是否还有更多页面
                link_header = response.headers.get('Link', '')
                if 'rel="next"' not in link_header:
                    break
                    
                page += 1
                time.sleep(1)  # 避免速率限制
                
            except requests.exceptions.RequestException as e:
                log(f"网络请求失败: {e}")
                break
            except Exception as e:
                log(f"处理仓库数据时出错: {e}")
                import traceback
                traceback.print_exc()
                break
        
        log(f"总共获取到 {len(repos)} 个 starred 仓库")
        return repos
    
    def should_sync_repo(self, repo_info: Dict) -> bool:
        """判断是否需要同步仓库"""
        if self.full_sync:
            return True
        
        repo_full_name = repo_info.get('full_name')
        repo_updated = repo_info.get('updated_at')
        
        if not repo_full_name or not repo_updated:
            return True
        
        # 检查状态文件中是否有记录
        if repo_full_name in self.state:
            repo_state = self.state[repo_full_name]
            last_synced = repo_state.get('last_updated')
            
            # 如果仓库未更新，则跳过
            if last_synced == repo_updated:
                return False
        
        return True
    
    def codeberg_repo_exists(self, repo_name: str) -> bool:
        """检查 Codeberg 上是否已存在仓库"""
        try:
            url = f"https://codeberg.org/api/v1/repos/{self.codeberg_username}/{repo_name}"
            response = requests.get(url, headers=self.codeberg_headers, timeout=10)
            return response.status_code == 200
        except Exception as e:
            log(f"检查 Codeberg 仓库失败: {e}")
            return False
    
    def create_codeberg_repo(self, repo_name: str, description: str = "") -> bool:
        """在 Codeberg 上创建仓库"""
        try:
            url = f"https://codeberg.org/api/v1/user/repos"
            data = {
                'name': repo_name,
                'description': description[:255] if description else f"GitHub mirror",
                'private': False
            }
            
            response = requests.post(
                url, 
                headers=self.codeberg_headers, 
                json=data,
                timeout=30
            )
            
            if response.status_code == 201:
                log(f"✓ 在 Codeberg 创建仓库: {repo_name}")
                return True
            elif response.status_code == 409:
                log(f"✓ 仓库已存在: {repo_name}")
                return True
            else:
                log(f"✗ 创建仓库失败 {response.status_code}: {response.text[:100]}")
                return False
                
        except Exception as e:
            log(f"✗ 创建 Codeberg 仓库时出错: {e}")
            return False
    
    def sync_repository(self, repo_info: Dict) -> bool:
        """简单智能方案"""
        repo_full_name = repo_info['full_name']
        original_name = repo_info['name']
        
        # 本地目录使用原始名称
        repo_path = self.repos_dir / original_name
        
        try:
            # 克隆或更新
            if repo_path.exists():
                run_command(['git', 'fetch', '--all'], cwd=repo_path)
                operation = "updated"
            else:
                clone_url = f"https://{self.github_token}@github.com/{repo_full_name}.git"
                run_command(['git', 'clone', '--mirror', clone_url, str(repo_path)])
                operation = "cloned"
            
            # 确定Codeberg仓库名
            if repo_full_name in self.state:
                # 使用历史记录的名称
                codeberg_name = self.state[repo_full_name].get('codeberg_name', original_name)
                log(f"  使用历史Codeberg名称: {codeberg_name}")
            else:
                # 首次尝试使用原始名称
                codeberg_name = original_name
                log(f"  首次尝试原始名称: {codeberg_name}")
            
            # 设置远程
            codeberg_url = f"https://{self.codeberg_username}:{self.codeberg_token}@codeberg.org/{self.codeberg_username}/{codeberg_name}.git"
            run_command(['git', 'remote', 'remove', 'codeberg'], cwd=repo_path, ignore_errors=True)
            run_command(['git', 'remote', 'add', 'codeberg', codeberg_url], cwd=repo_path)
            
            # 检查仓库是否存在
            if self.codeberg_repo_exists(codeberg_name):
                # 尝试非强制推送
                success, output = run_command(['git', 'push', 'codeberg', '--all'], cwd=repo_path)
            
                
                if not success and 'non-fast-forward' in output:
                    # 不是同一个仓库，需要新名称
                    log(f"  仓库冲突，生成新名称")
                    
                    # 使用用户名-仓库名格式
                    owner = repo_full_name.split('/')[0]
                    codeberg_name = f"{owner}-{original_name}"
                    
                    # 更新远程
                    codeberg_url = f"https://{self.codeberg_username}:{self.codeberg_token}@codeberg.org/{self.codeberg_username}/{codeberg_name}.git"
                    run_command(['git', 'remote', 'set-url', 'codeberg', codeberg_url], cwd=repo_path)
                    
                    # 创建新仓库并推送
                    if self.create_codeberg_repo(codeberg_name):
                        run_command(['git', 'push', 'codeberg', '--all', '--force'], cwd=repo_path)
                    else:
                        return False
            else:
                # 创建新仓库
                if self.create_codeberg_repo(codeberg_name):
                    run_command(['git', 'push', 'codeberg', '--all', '--force'], cwd=repo_path)
                else:
                    return False
            
            # 推送标签
            run_command(['git', 'push', 'codeberg', '--tags', '--force'], cwd=repo_path)
            
            # 更新状态
            self.state[repo_full_name] = {
                'name': original_name,
                'codeberg_name': codeberg_name,
                'last_updated': repo_info['updated_at'],
                'last_synced': datetime.now().isoformat(),
                'operation': operation
            }
            
            log(f"✓ 同步完成，Codeberg仓库: {codeberg_name}")
            return True
            
        except Exception as e:
            log(f"✗ 同步出错: {e}")
            return False
    
    def cleanup_old_repos(self, current_repos: List[Dict]):
        """清理已取消 star 的仓库状态"""
        current_names = {repo.get('full_name') for repo in current_repos if repo.get('full_name')}
        removed_count = 0
        
        for repo_name in list(self.state.keys()):
            if repo_name not in current_names:
                del self.state[repo_name]
                removed_count += 1
        
        if removed_count > 0:
            log(f"清理了 {removed_count} 个已取消 star 的仓库状态")
    
    def run(self):
        """主运行函数"""
        log("=" * 60)
        log("开始同步 starred 仓库到 Codeberg")
        log("状态文件: .github/sync_state.json")
        log("=" * 60)
        
        # 获取仓库列表
        repos = self.get_starred_repos()
        self.stats['total'] = len(repos)
        
        if not repos:
            log("没有找到 starred 仓库")
            return
        
        # 清理已取消 star 的仓库状态
        self.cleanup_old_repos(repos)
        
        log(f"找到 {len(repos)} 个仓库需要同步")
        log("-" * 60)
        
        # 同步每个仓库
        for i, repo in enumerate(repos, 1):
            repo_full_name = repo.get('full_name')
            
            if not repo_full_name:
                log(f"[{i}/{len(repos)}] 错误: 仓库信息不完整，跳过")
                self.stats['failed'] += 1
                continue
            
            log(f"[{i}/{len(repos)}] 处理: {repo_full_name}")
            
            # 检查是否需要同步
            if not self.should_sync_repo(repo):
                log(f"  仓库未更新，跳过")
                self.stats['skipped'] += 1
                continue
            
            # 执行同步
            if self.sync_repository(repo):
                repo_state = self.state.get(repo_full_name, {})
                if repo_state.get('operation') == 'cloned':
                    self.stats['new'] += 1
                else:
                    self.stats['updated'] += 1
            else:
                self.stats['failed'] += 1
            
            # 每处理 5 个仓库保存一次状态
            if i % 5 == 0:
                log("保存中间状态...")
                self.save_state()
            
            # 添加延迟避免速率限制
            time.sleep(2)
        
        # 最终保存状态
        self.save_state()
        
        # 显示统计
        log("=" * 60)
        log("同步完成!")
        log(f"总计仓库: {self.stats['total']}")
        log(f"新增仓库: {self.stats['new']}")
        log(f"更新仓库: {self.stats['updated']}")
        log(f"跳过仓库: {self.stats['skipped']}")
        log(f"失败仓库: {self.stats['failed']}")
        
        duration = datetime.now() - self.stats['start_time']
        minutes = int(duration.total_seconds() // 60)
        seconds = int(duration.total_seconds() % 60)
        log(f"总耗时: {minutes}分{seconds}秒")
        log("=" * 60)
        
        # 如果有失败，返回错误码
        if self.stats['failed'] > 0:
            sys.exit(1)

def main():
    """主函数"""
    try:
        sync = SyncManager()
        sync.run()
    except KeyboardInterrupt:
        log("\n操作被用户中断")
        sys.exit(1)
    except Exception as e:
        log(f"发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
