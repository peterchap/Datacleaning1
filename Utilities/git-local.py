import os

def check_local_git_repositories():
    locals = []
    for root, directories, files in os.walk('.'):
        if '.git' not in directories:
            #print(f'Found local git repository at {root}')
            locals.append(root)
    return locals

if __name__ == '__main__':
    local = check_local_git_repositories()
    file = open('no-git-local.txt', 'w', encoding='utf-8')
    for item in local:
        file.write(item+"\n")
    file.close()
